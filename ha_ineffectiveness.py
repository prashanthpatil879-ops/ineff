import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window
from typing import NamedTuple
from rda.engine.base_step import RDABaseStep
import rda.utils.spark_utils as su
import rda.utils.date_utils as du


class HAIneffectivenessInputs(NamedTuple):
    dv01_fi_df: DataFrame
    dv01_derivatives_df: DataFrame
    dv01_liab_post_hp_df: DataFrame
    fx_rates_df: DataFrame
    hierarchy_mapping_df: DataFrame
    rf_ear_shocks_df: DataFrame


class HAIneffectiveness(RDABaseStep):

    # Columns carried through the unpivot, before the tenor buckets are stacked.
    ALM_KEY_COLS = ["ASSET_TYPE", "INCEPTION_DATE", "PROGRAM_CODE", "LEGAL_ID", "SECURITY_CURRENCY", "SOURCE"]
    TENOR_COL_PATTERN = r"^\d+M$"

    # Everything is reported in CAD, so the IFS rates file carries no CAD row.
    BASE_CURRENCY = "CAD"

    # Columns on irr_ec_forecasting_ifs_fx_rates (curated). DATE is already a
    # proper date there, so no string parsing is needed.
    # BS_RATE = balance sheet (closing). IS_RATE = income statement (average).
    FX_CURRENCY_COL = "CODE"
    FX_RATE_COL = "BS_RATE"
    FX_DATE_COL = "DATE"

    # Tenors are doubles on both sides of the shocks join, so the key is rounded.
    TENOR_JOIN_DP = 6
    LOSSES_MULTIPLIER = 100

    ASSET_HOLDING_BOND = "Bond"
    ASSET_HOLDING_LIABILITY = "Liability"
    ASSET_HOLDING_MANUBANK = "Manubank"

    # Target column per asset holding on the losses table.
    LOSS_COLUMNS = {
        ASSET_HOLDING_LIABILITY: "LOSSES_LIAB_NO_GAMMA",
        ASSET_HOLDING_BOND: "LOSSES_HA_BOND",
        ASSET_HOLDING_MANUBANK: "LOSSES_MANUBANK",
    }

    LOSSES_GROUP_COLS = ["REPORT_DATE", "SECURITY_CURRENCY", "ASSET_HOLDING", "LOWEST_LEVEL_PORTFOLIO_NAME", "LEGAL_ID"]

    # HA DV01 mapping_NT_LLP.xlsx - hardcoded until the LLP mapping table is
    # ingested (FSD 2.4, ha_dv01_llp_csv.csv). Once it lands, read it in read()
    # and drop this constant with _llp_mapping_df().
    HA_DV01_LLP_MAPPING = [
        ("GH_14", 28, "USD", "HONG KONG SURPLUS"),
        ("MB_01", 58, "CAD", "ManuBank"),
        ("GH_01", 1, "CAD", "CA CAD-Guaranteed"),
        ("GH_09", 1, "CAD", "CA CAD-Guaranteed"),
        ("GH_01", 19, "USD", "JH USD-Guaranteed"),
        ("GH_09", 19, "USD", "JH USD-Guaranteed"),
        ("GH_01", 176, "AUD", "MLJ AUD-Guaranteed"),
        ("GH_01", 176, "JPY", "MLJ JPY-Guaranteed"),
        ("GH_09", 176, "JPY", "MLJ JPY-Guaranteed"),
        ("GH_01", 176, "USD", "MLJ USD-Guaranteed"),
        ("GH_01", 391, "AUD", "MLRL AUD-Guaranteed"),
        ("GH_01", 391, "JPY", "MLRL JPY-Guaranteed"),
        ("GH_09", 391, "JPY", "MLRL JPY-Guaranteed"),
        ("GH_01", 391, "USD", "MLRL USD-Guaranteed"),
        ("GH_02", 248, "USD", "CA CAD-Guaranteed"),
        ("GH_02", 176, "AUD", "MLJ JPY-Guaranteed"),
        ("GH_02", 176, "CAD", "MLJ JPY-Guaranteed"),
        ("GH_02", 176, "EUR", "MLJ JPY-Guaranteed"),
        ("GH_02", 176, "GBP", "MLJ JPY-Guaranteed"),
        ("GH_02", 176, "NOK", "MLJ JPY-Guaranteed"),
        ("GH_02", 176, "USD", "MLJ JPY-Guaranteed"),
        ("GH_02", 391, "AUD", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "CHF", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "EUR", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "GBP", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "NOK", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "USD", "MLRL JPY-Guaranteed"),
        ("GH_02", 391, "SEK", "MLRL JPY-Guaranteed"),
    ]

    HA_DV01_LLP_SCHEMA = "PROGRAM_CODE string, LEGAL_ID int, SECURITY_CURRENCY string, LOWEST_LEVEL_PORTFOLIO_NAME string"


    def read(self) -> HAIneffectivenessInputs:
        inputs = self._step_config.inputs
        last_quarter_end_date = self._date_ctx.asDict().get('PREVIOUSQUARTERENDDATE', None)
        self._logger.info(f"Reading inputs for HA Ineffectiveness process, Quarter End Date: {last_quarter_end_date}")

        if last_quarter_end_date is None:
            raise ValueError(f"Invalid date context entry for PREVIOUSQUARTERENDDATE in: {self._date_ctx.asDict()}")

        # ALM stamps a month's data on the 1st of the FOLLOWING month, so the
        # quarter ending 2026-03-31 arrives on the 2026-04-01 extract.
        next_month_first_day = du.get_next_month_first_day(last_quarter_end_date)

        # The curated FX table holds every month in one load, so the date filter
        # is what selects the quarter. read_watermark applies this filter BEFORE
        # taking MAX(INGESTION_TS), so the watermark is scoped to that month too.
        fx_filter = f"{self.FX_DATE_COL} = '{last_quarter_end_date}'"

        self._logger.info(f"ALM INCEPTION_DATE filter: {next_month_first_day}, FX / shocks date: {last_quarter_end_date}")

        input_dfs = HAIneffectivenessInputs(
            dv01_fi_df = self._uc.read_latest(
                inputs.dv01_fi.table_name,
                filter=f"INCEPTION_DATE='{next_month_first_day}'",
                watermark_col=inputs.dv01_fi.watermark_col
            ),
            dv01_derivatives_df = self._uc.read_latest(
                inputs.dv01_derivatives.table_name,
                filter=f"INCEPTION_DATE='{next_month_first_day}'",
                watermark_col=inputs.dv01_derivatives.watermark_col
            ),
            dv01_liab_post_hp_df = self._uc.read_latest(
                inputs.dv01_liab_post_hp.table_name,
                filter=f"INCEPTION_DATE='{next_month_first_day}'",
                watermark_col=inputs.dv01_liab_post_hp.watermark_col
            ),
            fx_rates_df = self._uc.read_latest(
                inputs.fx_rates.table_name,
                filter=fx_filter,
                watermark_col=inputs.fx_rates.watermark_col
            ),
            hierarchy_mapping_df = self._uc.read_latest(
                inputs.irr_mapping_heirarchy_mapping_latest.table_name,
                watermark_col=inputs.irr_mapping_heirarchy_mapping_latest.watermark_col
            ),
            rf_ear_shocks_df = self._uc.read_latest(
                inputs.rf_ear_shocks.table_name,
                filter=f"EFFECTIVE_DATE='{last_quarter_end_date}'",
                watermark_col=inputs.rf_ear_shocks.watermark_col
            )
        )

        self.validate_inputs(input_dfs)
        return input_dfs


    def unpivot(self, df: DataFrame) -> DataFrame:
        """Tenor bucket columns to rows. TENOR_CD is emitted in YEARS (003M -> 0.25)."""
        tenors = [c for c in df.columns if re.fullmatch(self.TENOR_COL_PATTERN, c)]

        if not tenors:
            raise ValueError(f"No tenor columns matching {self.TENOR_COL_PATTERN} found in: {df.columns}")

        self._logger.info(f"Unpivoting {len(tenors)} tenor columns to years: { {c: int(c[:-1]) / 12 for c in tenors} }")
        expr = "stack({0}, {1}) as (TENOR_CD, DV01_VALUE)".format(
            len(tenors), ", ".join([f"CAST({int(c[:-1]) / 12} AS DOUBLE), CAST(`{c}` AS DOUBLE)" for c in tenors])
        )
        return df.select(*self.ALM_KEY_COLS, F.expr(expr))


    def validate_lookup(self, df: DataFrame, lookup_col: str, key_cols: list, source: str) -> None:
        """Warn on unmatched keys and carry on. Matched rows are unaffected."""
        unmatched = df.filter(F.col(lookup_col).isNull()).select(*key_cols).distinct().collect()

        if unmatched:
            self._logger.warning(
                f"{source} lookup: no match for {len(unmatched)} key(s) {[r.asDict() for r in unmatched]}. "
                f"These rows keep a null {lookup_col}; all other rows are unaffected."
            )
        else:
            self._logger.info(f"{source} lookup matched every row")


    def _llp_mapping_df(self) -> DataFrame:
        return self._spark.createDataFrame(self.HA_DV01_LLP_MAPPING, self.HA_DV01_LLP_SCHEMA).distinct()


    def calculate_partial_dv01(self, inputs: HAIneffectivenessInputs) -> DataFrame:
        self._logger.info("Starting HA Ineffectiveness - Partial DV01 calculation process...")
        last_quarter_end_date = self._date_ctx.asDict().get('PREVIOUSQUARTERENDDATE')

        dv01_unpivot_df = (
            self.unpivot(inputs.dv01_fi_df)
            .unionByName(self.unpivot(inputs.dv01_derivatives_df))
            .unionByName(self.unpivot(inputs.dv01_liab_post_hp_df))
            .withColumn("DV01_VALUE", F.coalesce(F.col("DV01_VALUE"), F.lit(0.0)))
            .withColumn("INCEPTION_DATE", F.to_date(F.col("INCEPTION_DATE")))
            .withColumn("PROGRAM_CODE", F.upper(F.trim(F.col("PROGRAM_CODE"))))
            .withColumn("SECURITY_CURRENCY", F.upper(F.trim(F.col("SECURITY_CURRENCY"))))
            .withColumn("LEGAL_ID", F.col("LEGAL_ID").cast("int"))
        )

        dv01_unpivot_df = self.cache_df(dv01_unpivot_df, "dv01_unpivot_df")
        self.write(dv01_unpivot_df, self._step_config.step_dv01_unpivot.table_name, self._step_config.step_dv01_unpivot.ext_table_loc, mode=RDABaseStep.spark_write_mode)

        # TOTAL_DV01 = sum of DV01 across the three ALM datasets, per tenor. The
        # datasets are disjoint on PROGRAM_CODE (GH_01 only in liab_post_hp +
        # derivatives, the rest only in fi + derivatives), so this reproduces the
        # per-combination source rules in HA DV01 mapping_NT_LLP.xlsx.
        # REPORT_DATE is the quarter end the extract belongs to.
        total_dv01_df = (
            dv01_unpivot_df
            .withColumn("REPORT_DATE", F.lit(last_quarter_end_date).cast("date"))
            .groupBy("REPORT_DATE", "PROGRAM_CODE", "LEGAL_ID", "SECURITY_CURRENCY", "TENOR_CD")
            .agg( F.sum("DV01_VALUE").alias("TOTAL_DV01") )
        )

        # LLP from the mapping, then TAX_RATE from the hierarchy mapping keyed on
        # that LLP. su.normalize_text absorbs the case difference between the
        # mapping ("ManuBank") and the hierarchy table ("Manubank").
        tax_rate_df = (
            inputs.hierarchy_mapping_df
            .withColumn("rnk", F.dense_rank().over(Window.partitionBy(su.normalize_text(F.col("LOWEST_LEVEL_PORTFOLIO_NAME"))).orderBy(F.col("REPORTING_DATE_KEY").desc())))
            .filter( F.col("rnk") == 1 )
            .select(
                su.normalize_text(F.col("LOWEST_LEVEL_PORTFOLIO_NAME")).alias("LLP_KEY"),
                F.col("TAX_RATE").cast("double").alias("TAX_RATE")
            )
            .dropDuplicates(["LLP_KEY"])
        )

        dv01_with_llp_df = (
            total_dv01_df.alias("d")
            .join(
                self._llp_mapping_df().alias("m"),
                on = ["PROGRAM_CODE", "LEGAL_ID", "SECURITY_CURRENCY"],
                how = "left"
            )
            .select("d.*", "m.LOWEST_LEVEL_PORTFOLIO_NAME")
        )
        self.validate_lookup(dv01_with_llp_df, "LOWEST_LEVEL_PORTFOLIO_NAME", ["PROGRAM_CODE", "LEGAL_ID", "SECURITY_CURRENCY"], "LLP mapping")

        dv01_with_tax_df = (
            dv01_with_llp_df.alias("d")
            .join(
                tax_rate_df.alias("t"),
                on = su.normalize_text(F.col("d.LOWEST_LEVEL_PORTFOLIO_NAME")) == F.col("t.LLP_KEY"),
                how = "left"
            )
            .select("d.*", "t.TAX_RATE")
        )
        self.validate_lookup(dv01_with_tax_df, "TAX_RATE", ["LOWEST_LEVEL_PORTFOLIO_NAME"], "Tax rate")

        fx_rate_df = (
            inputs.fx_rates_df
            .select(
                F.upper(F.trim(F.col(self.FX_CURRENCY_COL))).alias("FX_CURRENCY"),
                F.col(self.FX_RATE_COL).cast("double").alias("FX_RATE")
            )
            .dropDuplicates(["FX_CURRENCY"])
        )

        dv01_with_fx_df = (
            dv01_with_tax_df.alias("d")
            .join(
                fx_rate_df.alias("f"),
                on = F.col("d.SECURITY_CURRENCY") == F.col("f.FX_CURRENCY"),
                how = "left"
            )
            .select("d.*", "f.FX_RATE")
            # The reporting currency has no row in the rates file; give it 1.
            # Scoped to BASE_CURRENCY only - any other unmatched currency stays
            # null and is reported below, because defaulting something like JPY
            # to 1.0 would misstate it by roughly 100x.
            .withColumn(
                "FX_RATE",
                F.when( F.col("FX_RATE").isNull() & (F.col("SECURITY_CURRENCY") == F.lit(self.BASE_CURRENCY)), F.lit(1.0) )
                 .otherwise( F.col("FX_RATE") )
            )
        )
        self.validate_lookup(dv01_with_fx_df, "FX_RATE", ["SECURITY_CURRENCY"], "FX rate")

        partial_dv01_df = (
            dv01_with_fx_df
            .withColumn(
                "ASSET_HOLDING",
                F.when( F.col("PROGRAM_CODE").isin("GH_14", "GH_09", "GH_02"), self.ASSET_HOLDING_BOND )
                 .when( F.col("PROGRAM_CODE") == "GH_01", self.ASSET_HOLDING_LIABILITY )
                 .when( F.col("PROGRAM_CODE") == "MB_01", self.ASSET_HOLDING_MANUBANK )
            )
            .withColumn("SUM_PARTIAL_DV01", F.col("TOTAL_DV01") * F.col("FX_RATE") * (1 - F.col("TAX_RATE")))
            .select(
                "REPORT_DATE", "SECURITY_CURRENCY", "ASSET_HOLDING", "TENOR_CD",
                "PROGRAM_CODE", "LOWEST_LEVEL_PORTFOLIO_NAME", "LEGAL_ID",
                "TOTAL_DV01", "FX_RATE", "TAX_RATE", "SUM_PARTIAL_DV01"
            )
        )
        self.validate_lookup(partial_dv01_df, "ASSET_HOLDING", ["PROGRAM_CODE"], "Asset holding")

        # Left joins never drop rows, so the grain must still equal TOTAL_DV01.
        # A mismatch means a mapping has duplicate keys and DV01 is double counted.
        if partial_dv01_df.count() != total_dv01_df.count():
            raise ValueError("Row count changed across the lookups - a mapping has duplicate keys")

        partial_dv01_df = self.cache_df(partial_dv01_df, "partial_dv01_df")

        self.write(partial_dv01_df, self._step_config.step_partial_dv01.table_name, self._step_config.step_partial_dv01.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("HA Ineffectiveness - Partial DV01 calculation completed")
        return partial_dv01_df


    def calculate_losses(self, inputs: HAIneffectivenessInputs, partial_dv01_df: DataFrame) -> DataFrame:
        self._logger.info("Starting HA Ineffectiveness - Losses calculation process...")

        # Collapse to one DV01 per key + tenor before the shocks are applied, in
        # case a key ever carries more than one PROGRAM_CODE.
        dv01_df = (
            partial_dv01_df
            .groupBy(*self.LOSSES_GROUP_COLS, "TENOR_CD")
            .agg( F.sum("SUM_PARTIAL_DV01").alias("SUM_PARTIAL_DV01") )
            .withColumn("TENOR_KEY", F.round(F.col("TENOR_CD"), self.TENOR_JOIN_DP))
        )

        rf_ear_shocks_base_df = (
            inputs.rf_ear_shocks_df
            .select(
                F.upper(F.trim(F.col("CURRENCY"))).alias("SHOCK_CURRENCY"),
                F.round(F.col("TENOR").cast("double"), self.TENOR_JOIN_DP).alias("TENOR_KEY"),
                F.col("SCENARIO").cast("int").alias("SCENARIO"),
                F.col("SHOCKS").cast("double").alias("SHOCKS")
            )
            .dropDuplicates(["SHOCK_CURRENCY", "TENOR_KEY", "SCENARIO"])
        )

        # Each tenor is multiplied by the shock for THAT SAME tenor and scenario;
        # the products are then summed across the curve and scaled by 100.
        # dv01_df is tiny against millions of shock rows, so it is broadcast.
        losses_base_df = (
            F.broadcast(dv01_df).alias("d")
            .join(
                rf_ear_shocks_base_df.alias("s"),
                on = [
                    F.col("d.SECURITY_CURRENCY") == F.col("s.SHOCK_CURRENCY"),
                    F.col("d.TENOR_KEY") == F.col("s.TENOR_KEY")
                ],
                how = "left"
            )
            .select("d.*", "s.SCENARIO", "s.SHOCKS")
        )

        unmatched = (
            losses_base_df.filter( F.col("SHOCKS").isNull() )
            .groupBy("SECURITY_CURRENCY")
            .agg( F.sort_array(F.collect_set("TENOR_CD")).alias("TENORS") )
            .collect()
        )
        if unmatched:
            self._logger.warning(
                f"No shocks found for { {r['SECURITY_CURRENCY']: r['TENORS'] for r in unmatched} }. "
                f"Those currency/tenor combinations contribute nothing, and any key left with "
                f"no shocks at all produces no rows."
            )

        # All three loss columns come from one pass: sum only the products
        # belonging to that asset holding. A key with no rows for an asset
        # holding gets null, not zero.
        losses_amt_df = (
            losses_base_df
            .filter( F.col("SCENARIO").isNotNull() )
            .withColumn("TENOR_PRODUCT", F.col("SUM_PARTIAL_DV01") * F.col("SHOCKS"))
            .groupBy(*self.LOSSES_GROUP_COLS, "SCENARIO")
            .agg(
                *[
                    ( F.sum(F.when(F.col("ASSET_HOLDING") == holding, F.col("TENOR_PRODUCT"))) * F.lit(self.LOSSES_MULTIPLIER) ).alias(column)
                    for holding, column in self.LOSS_COLUMNS.items()
                ],
                *[
                    F.count(F.when(F.col("ASSET_HOLDING") == holding, F.col("TENOR_PRODUCT"))).alias(f"{column}_TENORS")
                    for holding, column in self.LOSS_COLUMNS.items()
                ]
            )
        )

        # Every populated column must be built from the full tenor curve. A count
        # between 1 and expected-1 means a tenor was lost in the shocks join.
        expected_tenors = partial_dv01_df.select("TENOR_CD").distinct().count()

        incomplete_curve = None
        for column in self.LOSS_COLUMNS.values():
            check = ~F.col(f"{column}_TENORS").isin(0, expected_tenors)
            incomplete_curve = check if incomplete_curve is None else (incomplete_curve | check)

        partial_curves = losses_amt_df.filter(incomplete_curve).count()
        if partial_curves:
            self._logger.warning(f"{partial_curves} row(s) summed fewer than the full {expected_tenors}-tenor curve.")
        else:
            self._logger.info(f"Every populated loss column summed the full {expected_tenors}-tenor curve")

        losses_df = losses_amt_df.select(
            "REPORT_DATE",
            "SECURITY_CURRENCY",
            "ASSET_HOLDING",
            "LOWEST_LEVEL_PORTFOLIO_NAME",
            "LEGAL_ID",
            "SCENARIO",
            *self.LOSS_COLUMNS.values()
        )

        losses_df = self.cache_df(losses_df, "losses_df")

        self.write(losses_df, self._step_config.step_losses.table_name, self._step_config.step_losses.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("HA Ineffectiveness - Losses calculation completed")
        return losses_df
