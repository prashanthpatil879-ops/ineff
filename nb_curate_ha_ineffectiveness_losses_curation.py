# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../../common/nb_load_rda_package

# COMMAND ----------

from rda.utils.logger import get_notebook_logger
logger = get_notebook_logger()
logger.info("HA Ineffectiveness - Losses curation loading started...")

# COMMAND ----------

dataset_key = "ha_ineffectiveness_losses_curation"

# COMMAND ----------

dataset_cfg = env_config.get(f"curation.{dataset_key}")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from rda.io.unity_catalog import UnityCatalog
from rda.utils.date_utils import next_quarter_end

uc = UnityCatalog(env_config, spark)

# Tenor values are doubles on both sides, so the join key is rounded to avoid any
# float representation mismatch (0.25 / 0.5 are exact, a future 1M bucket at
# 0.08333... would not be).
TENOR_JOIN_DP = 6
LOSSES_MULTIPLIER = 100

# COMMAND ----------

partial_dv01_df = uc.read_latest(dataset_cfg.inputs.partial_dv01)

report_date = partial_dv01_df.agg(F.max("REPORT_DATE").alias("D")).collect()[0]["D"]
if report_date is None:
    raise ValueError(f"No REPORT_DATE found on {dataset_cfg.inputs.partial_dv01}")
logger.info(f"Reporting date from partial DV01: {report_date}")

# COMMAND ----------

# Shocks are quarterly, so EFFECTIVE_DATE is always a quarter end. The reporting
# date is mapped onto the quarter end it falls in - date_utils.next_quarter_end
# returns the END OF THE QUARTER CONTAINING the date, so it covers both cases in
# one step:
#   REPORT_DATE 2026-06-30 (a quarter end) -> 2026-06-30, an exact match
#   REPORT_DATE 2026-04-30 (off cycle)     -> 2026-06-30, that quarter's shocks
# If that quarterly set has not been loaded yet, fall back to the latest set
# present and say which one was used.
shocks_table = dataset_cfg.inputs.rf_ear_shocks
quarter_end = next_quarter_end(report_date)

available_dates = sorted(
    r["EFFECTIVE_DATE"] for r in
    uc.read_query(f"SELECT DISTINCT CAST(EFFECTIVE_DATE AS DATE) AS EFFECTIVE_DATE FROM {shocks_table}").collect()
    if r["EFFECTIVE_DATE"] is not None
)

if not available_dates:
    raise ValueError(f"{shocks_table} has no EFFECTIVE_DATE values")

logger.info(f"Report date {report_date} sits in the quarter ending {quarter_end}. Shock sets available: {available_dates}")

if quarter_end in available_dates:
    effective_date = quarter_end
else:
    effective_date = max(available_dates)
    logger.warning(
        f"No shocks with EFFECTIVE_DATE = {quarter_end} yet. Using the latest set on file, {effective_date}."
    )

# The EFFECTIVE_DATE filter is applied BEFORE the INGESTION_TS watermark is taken,
# so this returns the latest load OF THAT QUARTER, not simply the newest load in
# the table. Relying on read_latest alone would pin to whichever quarter happened
# to be written most recently.
rf_ear_shocks_df = uc.read_latest(shocks_table, filter=f"EFFECTIVE_DATE = '{effective_date}'")
logger.info(f"Shocks: EFFECTIVE_DATE {effective_date}, {rf_ear_shocks_df.select('SCENARIO').distinct().count()} scenarios")

# COMMAND ----------

def sum_product_losses(partial_dv01_df, shocks_df, asset_holding):
    """
    STM: [ Sum over tenors of (sum_partial_dv01 * shocks) ] * 100, per currency,
    LLP and scenario, for one asset_holding.

    Each tenor is multiplied by the shock for THAT SAME tenor, then the ten
    products are summed - it is a sum-product across the tenor curve, not one
    DV01 applied to every shock.
    """
    dv01_df = (
        partial_dv01_df
        .filter( F.col("ASSET_HOLDING") == asset_holding )
        # A currency + LLP can carry more than one PROGRAM_CODE / LEGAL_ID, so
        # collapse to one figure per tenor before applying the shocks.
        .groupBy("REPORT_DATE", "SECURITY_CURRENCY", "LOWEST_LEVEL_PORTFOLIO_NAME", "TENOR_CD")
        .agg( F.sum("SUM_PARTIAL_DV01").alias("SUM_PARTIAL_DV01") )
        .withColumn("TENOR_KEY", F.round(F.col("TENOR_CD"), TENOR_JOIN_DP))
    )

    shocks_base_df = (
        shocks_df
        .select(
            F.upper(F.trim(F.col("CURRENCY"))).alias("SHOCK_CURRENCY"),
            F.round(F.col("TENOR").cast("double"), TENOR_JOIN_DP).alias("TENOR_KEY"),
            F.col("SCENARIO").cast("int").alias("SCENARIO"),
            F.col("SHOCKS").cast("double").alias("SHOCKS")
        )
        .dropDuplicates(["SHOCK_CURRENCY", "TENOR_KEY", "SCENARIO"])
    )

    # dv01_df is tiny (tens of rows) against millions of shock rows, so broadcast it.
    joined_df = (
        F.broadcast(dv01_df).alias("d")
        .join(
            shocks_base_df.alias("s"),
            on = [
                F.col("d.SECURITY_CURRENCY") == F.col("s.SHOCK_CURRENCY"),
                F.col("d.TENOR_KEY") == F.col("s.TENOR_KEY")
            ],
            how = "left"
        )
        .select("d.*", "s.SCENARIO", "s.SHOCKS")
    )

    unmatched = (
        joined_df.filter( F.col("SHOCKS").isNull() )
        .groupBy("SECURITY_CURRENCY")
        .agg( F.sort_array(F.collect_set("TENOR_CD")).alias("TENORS") )
        .collect()
    )
    if unmatched:
        logger.warning(
            f"{asset_holding}: no shocks found for { {r['SECURITY_CURRENCY']: r['TENORS'] for r in unmatched} }. "
            f"Those currency/tenor combinations contribute nothing, and any LLP left with no shocks at all "
            f"produces no rows for this column."
        )

    return (
        joined_df
        .filter( F.col("SCENARIO").isNotNull() )
        .withColumn("TENOR_PRODUCT", F.col("SUM_PARTIAL_DV01") * F.col("SHOCKS"))
        .groupBy("REPORT_DATE", "SECURITY_CURRENCY", "LOWEST_LEVEL_PORTFOLIO_NAME", "SCENARIO")
        .agg(
            ( F.sum("TENOR_PRODUCT") * F.lit(LOSSES_MULTIPLIER) ).alias("LOSSES"),
            F.count("TENOR_PRODUCT").alias("TENOR_COUNT")
        )
    )

# COMMAND ----------

losses_liab_df = sum_product_losses(partial_dv01_df, rf_ear_shocks_df, "Liability")

# Every scenario should be built from the full tenor curve. An LLP short of the
# expected count has lost a tenor somewhere in the join.
tenor_counts = [r["TENOR_COUNT"] for r in losses_liab_df.select("TENOR_COUNT").distinct().collect()]
expected_tenors = partial_dv01_df.select("TENOR_CD").distinct().count()
if tenor_counts != [expected_tenors]:
    logger.warning(f"Expected {expected_tenors} tenors per scenario, found counts {sorted(tenor_counts)}.")
else:
    logger.info(f"Every scenario summed the full {expected_tenors}-tenor curve")

losses_df = (
    losses_liab_df
    .select(
        "REPORT_DATE",
        "SECURITY_CURRENCY",
        "SCENARIO",
        "LOWEST_LEVEL_PORTFOLIO_NAME",
        F.col("LOSSES").alias("LOSSES_LIAB_NO_GAMMA")
    )
)

logger.info(f"Losses rows: {losses_df.count()}")
display(losses_df.orderBy("SECURITY_CURRENCY", "LOWEST_LEVEL_PORTFOLIO_NAME", "SCENARIO"))

# COMMAND ----------

losses_df = losses_df.withColumn("INGESTION_TS", F.from_utc_timestamp(F.current_timestamp(), 'America/Toronto'))
uc.write(losses_df, dataset_cfg.curation_table, dataset_cfg.curation_ext_table_loc)

# COMMAND ----------

logger.info("HA Ineffectiveness - Losses curation loading completed")
