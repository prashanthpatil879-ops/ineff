from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window
from typing import NamedTuple
from rda.engine.base_step import RDABaseStep
import rda.utils.spark_utils as su


class NonHAInputs(NamedTuple):
    dv01_all_asset_derivative_df: DataFrame
    dim_tenors: DataFrame
    rf_ear_shocks_df: DataFrame
    hierarchy_mapping_df: DataFrame
    asset_summary_df: DataFrame
    exlusion_mapping_df: DataFrame
    avg_shocks_df: DataFrame


class NonHA(RDABaseStep):

    def read(self) -> NonHAInputs:
        inputs = self._step_config.inputs
        last_quarter_end_date = self._date_ctx.asDict().get('PREVIOUSQUARTERENDDATE', None)
        self._logger.info(f"Reading inputs for Non HA process, Quarter End Date: {last_quarter_end_date}")

        if last_quarter_end_date is None:
            raise ValueError(f"Invalid date context entry for PREVIOUSQUARTERENDDATE in: {self._date_ctx.asDict()}")

        input_dfs = NonHAInputs(
            dv01_all_asset_derivative_df = self._uc.read_latest(
                inputs.dv01_all_asset_derivative.table_name,
                filter=f"REPORT_DATE='{last_quarter_end_date}' AND TAM_MARKET in ('ASIA', 'SURPLUS', 'REINSURANCE', 'US', 'CANADA') AND HEDGE_PROGRAM is NULL",
                watermark_col=inputs.dv01_all_asset_derivative.watermark_col
            ), 
            dim_tenors = self._uc.read_latest(
                inputs.dim_tenors.table_name,
                filter=f"IS_ACTIVE=true",
                watermark_col=inputs.dim_tenors.watermark_col
            ),
            rf_ear_shocks_df = self._uc.read_latest(
                inputs.rf_ear_shocks.table_name,
                filter=f"EFFECTIVE_DATE='{last_quarter_end_date}'",
                watermark_col=inputs.rf_ear_shocks.watermark_col
            ),
            hierarchy_mapping_df = self._uc.read_latest(
                inputs.irr_mapping_heirarchy_mapping_latest.table_name,
                watermark_col=inputs.irr_mapping_heirarchy_mapping_latest.watermark_col
            ),
            asset_summary_df = self._uc.read_latest(
                inputs.asset_summary_bd09.table_name,
                filter=f"REPORT_DATE='{last_quarter_end_date}' AND TAM_MARKET in ('ASIA', 'SURPLUS', 'REINSURANCE', 'US', 'CANADA') AND HEDGE_PROGRAM is NULL AND IR_SCENARIO_NAME in ('PARALLEL_+50BPS', 'PARALLEL_-50BPS') AND QIS_SCENARIO_CATEGORY='RiskFree'",
                watermark_col=inputs.asset_summary_bd09.watermark_col
            ),
            exlusion_mapping_df = self._uc.read_latest(
                inputs.exlusion_mapping.table_name,
                watermark_col=inputs.exlusion_mapping.watermark_col
            ),
            avg_shocks_df = self._uc.read_latest(
                inputs.avg_shocks.table_name,
                filter=f"REPORTING_DATE='{last_quarter_end_date}'",
                watermark_col=inputs.avg_shocks.watermark_col
            )
        )

        self.validate_inputs(input_dfs)
        return input_dfs


    def post_tax_calculation(self, inputs: NonHAInputs) -> DataFrame:
        self._logger.info(f"Staring Non HA - Post Tax calculation process...")

        dv01_all_asset_derivative_base_df = (
            inputs.dv01_all_asset_derivative_df
            .select('TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY', 'RISK_FACTOR_NAME', 'DELTA_CAD')
        )

        dv01_all_asset_derivative_with_tenors_df = (
            dv01_all_asset_derivative_base_df.alias('data')
            .join(
                inputs.dim_tenors.alias('tenors'),
                on = F.upper(F.col('data.RISK_FACTOR_NAME')) == F.upper(F.col('tenors.TENOR_CODE')),
                how = 'inner'
            )
            .select('TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY', 'tenors.TENOR_MONTHS', 'DELTA_CAD')
        )

        dv01_posttax_df = (
            dv01_all_asset_derivative_with_tenors_df
            .groupBy('TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY', 'TENOR_MONTHS')
            .agg( F.sum('DELTA_CAD').alias('SUM_POST_TAX') )
        )

        dv01_posttax_df = dv01_posttax_df.withColumn("HEDGE_PROGRAM", F.lit(None))
        dv01_posttax_df = self.cache_df(dv01_posttax_df, "dv01_posttax_df")

        self.write(dv01_posttax_df, self._step_config.step_sum_post_tax.table_name, self._step_config.step_sum_post_tax.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("Non HA - Post Tax calculation process completed")
        return dv01_posttax_df
    
    
    def calculate_losses(self, inputs: NonHAInputs, dv01_posttax_df) -> DataFrame:
        self._logger.info(f"Staring Non HA - Losses calculation process...")

        dv01_posttax_df = dv01_posttax_df.drop('HEDGE_PROGRAM')
        dv01_posttax_df = (
            dv01_posttax_df
            .withColumn('ISSUE_CURRENCY', F.when( F.col('ISSUE_CURRENCY') == 'CNH', 'CNY').otherwise(F.col('ISSUE_CURRENCY')) )
        )        

        rf_ear_shocks_base_df = (
            inputs.rf_ear_shocks_df.alias('rf')
            .join(
                inputs.dim_tenors.alias('tenors'),
                on = F.col('rf.TENOR') == F.col('tenors.TENOR_YEARS'),
                how = 'inner'
            )
            .select('rf.CURRENCY', 'tenors.TENOR_MONTHS', 'rf.SCENARIO', 'rf.SHOCKS')
        )

        calculate_amt_tmp_df = (
            dv01_posttax_df.alias('post_tax')
            .join(
                rf_ear_shocks_base_df.alias('s'),
                on = [F.upper(F.col('post_tax.ISSUE_CURRENCY')) == F.upper(F.col('s.CURRENCY')), F.col('post_tax.TENOR_MONTHS') == F.col('s.TENOR_MONTHS')],
                how = 'inner'
            )
            .select('post_tax.*', 's.SCENARIO', 's.SHOCKS')       
        )

        calculate_amt_df = (
            calculate_amt_tmp_df
            .groupBy('TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY', 'SCENARIO')
            .agg( F.sum(F.col('SUM_POST_TAX') * F.col('SHOCKS')).alias('AMOUNT') )
            .withColumn('AMOUNT', F.col('AMOUNT')/10 )
        )

        calculate_cl95_df = (
            calculate_amt_df
            .groupBy('TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY')
            .agg( F.percentile_approx(F.col('AMOUNT'), 0.95).alias('CL95_EAR') )
        )

        amt_with_cl95_df = (
            calculate_amt_df.alias('a')
            .join(
                calculate_cl95_df.alias('b'),
                on = ['TAM_MARKET', 'TAM_SEGMENT', 'ISSUE_CURRENCY'],
                how = 'inner'
            )
            .select('a.*', 'b.CL95_EAR')
        )

        amt_with_cl95_with_llp_df = (
            amt_with_cl95_df.alias('amt')
            .join(
                inputs.hierarchy_mapping_df.alias('h'),
                on = su.normalize_text(F.col('amt.TAM_SEGMENT')) == su.normalize_text(F.col('h.TAM_SEGMENT_NAME')), 
                how = 'inner'
            )
            .withColumn('SEGMENT_ISSUECURRENCY', F.concat(F.col('amt.TAM_MARKET'), F.lit(''), F.col('amt.ISSUE_CURRENCY')))
            .select(F.col('amt.TAM_MARKET').alias('SEGMENT'), 'amt.TAM_SEGMENT', 'h.LOWEST_LEVEL_PORTFOLIO_NAME', 'amt.ISSUE_CURRENCY', 'SEGMENT_ISSUECURRENCY', 'amt.SCENARIO', 'amt.AMOUNT', 'amt.CL95_EAR')
        )

        amt_with_cl95_with_llp_df = amt_with_cl95_with_llp_df.withColumn("HEDGE_PROGRAM", F.lit(None))
        amt_with_cl95_with_llp_df = self.cache_df(amt_with_cl95_with_llp_df, "amt_with_cl95_with_llp_df")

        self.write(amt_with_cl95_with_llp_df, self._step_config.step_losses.table_name, self._step_config.step_losses.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("Non HA - Calculation of losses completed")
        return amt_with_cl95_with_llp_df
    

    def calculate_ir_gamma(self, inputs: NonHAInputs) -> DataFrame: 
        self._logger.info("Non HA - Calculating IR Gamma")
        
        asset_summary_base_df = (
            inputs.asset_summary_df.alias("alm_asset")
            .join(
                inputs.hierarchy_mapping_df.alias("h"),
                on = su.normalize_text(F.col("alm_asset.TAM_SEGMENT")) == su.normalize_text(F.col("h.TAM_SEGMENT_NAME")),
                how = "inner"
            )
            .join(
                inputs.exlusion_mapping_df.alias("ex"),
                on = su.normalize_text(F.col("alm_asset.TAM_SEGMENT")) == su.normalize_text(F.col("ex.TAM_SEGMENT")),
                how = "inner"
            )
            # @TODO: Remove below filters before checking in the code
            #.filter( F.col("h.PRODUCT_TYPE_NAME").isin('Non-par guaranteed', 'Non-par adjustable/guaranteed mixed', 'Surplus', 'Non-par adjustable') )
            .filter( F.col("ex.EXCLUDE") == "No"  )
            .select("alm_asset.*", "h.LOWEST_LEVEL_PORTFOLIO_NAME", "h.TAX_RATE", "ex.EXCLUDE")
        )

        asset_summary_base_df = (
            asset_summary_base_df
            .withColumn('ISSUE_CURRENCY', F.when( F.col('ISSUE_CURRENCY') == 'CNH', 'CNY').otherwise(F.col('ISSUE_CURRENCY')) )
            .withColumn('SEGMENT_ISSUECURRENCY', F.concat(F.col('TAM_MARKET'), F.lit(''), F.col('ISSUE_CURRENCY')))
        )

        ir_gamma_derive_df = (
            asset_summary_base_df
            .withColumn(
                "SEGMENT",
                F.when(
                    F.col("TAM_SEGMENT") == "MLRL USD-SG Index UL",
                    "REINSURANCE"
                )
                .when(
                    (F.upper(F.col("TAM_SEGMENT")).endswith("SURPLUS")) |
                    (F.upper(F.col("TAM_MARKET")) == "SURPLUS"),
                    "SURPLUS"
                )
                .when(
                    F.upper(F.col("TAM_COUNTRY")) == "CANADA",
                    "CANADA"
                )
                .when(
                    F.upper(F.col("TAM_COUNTRY")) == "US",
                    "US"
                )
                .otherwise("ASIA")
            )
            .withColumn(
                "POST_TAX_SENSITIVITY_CAD",
                F.col("SENSITIVITY_CAD") * ( 1 - F.col("TAX_RATE")) / 1000000
            )
            .select("SEGMENT", "TAM_SEGMENT", "LOWEST_LEVEL_PORTFOLIO_NAME", "ISSUE_CURRENCY", "SEGMENT_ISSUECURRENCY", "IR_SCENARIO_NAME", "POST_TAX_SENSITIVITY_CAD")
        ) 

        ir_gamma_df = (
            ir_gamma_derive_df
            .groupBy("SEGMENT", "TAM_SEGMENT", "LOWEST_LEVEL_PORTFOLIO_NAME", "ISSUE_CURRENCY", "SEGMENT_ISSUECURRENCY")
            .pivot("IR_SCENARIO_NAME")
            .agg( F.sum("POST_TAX_SENSITIVITY_CAD") )
            .withColumn("IR_GAMMA", (F.col("PARALLEL_+50BPS") + F.col("PARALLEL_-50BPS")) * 4 )
        )

        ir_gamma_df = self.cache_df(ir_gamma_df, "ir_gamma_df")

        self.write(ir_gamma_df, self._step_config.step_ir_gamma.table_name, self._step_config.step_ir_gamma.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("Non HA - IR Gamma calculation completed")

        return ir_gamma_df
        
    
    def calculate_losses_with_gamma(self, inputs: NonHAInputs, ir_gamma_df) -> DataFrame:
        self._logger.info("Non HA - Calculating losses with gamma")
        
        losses_with_gamma_base = (
            ir_gamma_df.alias("i")
            .join(
                inputs.avg_shocks_df.alias("s"),
                on = F.upper(F.col("i.ISSUE_CURRENCY")) == F.upper(F.col("s.CURRENCY")),
                how = "inner"
            )
            .select("i.*", "s.SCENARIO", "s.AVG_SHOCKS")
        )

        losses_with_gamma_amt = (
            losses_with_gamma_base
            .withColumn(
                "AMOUNT", (F.pow(F.col("AVG_SHOCKS"), 2)) * F.col("IR_GAMMA") * 0.5
            )
        )

        losses_with_gamma_cl95 = (
            losses_with_gamma_amt
            .groupBy("SEGMENT", "TAM_SEGMENT", "LOWEST_LEVEL_PORTFOLIO_NAME", "ISSUE_CURRENCY", "SEGMENT_ISSUECURRENCY")
            .agg( F.percentile_approx(F.col("AMOUNT"), 0.95).alias("CL95_EAR") )
        )

        losses_with_gamma = (
            losses_with_gamma_amt.alias("a")
            .join(
                losses_with_gamma_cl95.alias("b"), 
                on = ["SEGMENT", "TAM_SEGMENT", "LOWEST_LEVEL_PORTFOLIO_NAME", "ISSUE_CURRENCY", "SEGMENT_ISSUECURRENCY"],
                how = "inner"
            )
            .select("a.*", "b.CL95_EAR")
        )

        losses_with_gamma = self.cache_df(losses_with_gamma, "losses_with_gamma")

        self.write(losses_with_gamma, self._step_config.step_losses_with_gamma.table_name, self._step_config.step_losses_with_gamma.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("Non HA - Losses with gamma calculation completed")

        return losses_with_gamma
    

    def calculate_total_losses(self, inputs: NonHAInputs, amt_with_cl95_with_llp_df, losses_with_gamma) -> DataFrame:
        self._logger.info("Non HA - Calculating total losses")

        total_losses_df = (
            amt_with_cl95_with_llp_df.alias("a")
            .join(
                losses_with_gamma.alias("b"),
                on = (
                        (F.col("a.SEGMENT") == F.col("b.SEGMENT"))
                        & (F.col("a.TAM_SEGMENT") == F.col("b.TAM_SEGMENT"))
                        & ( su.normalize_text( F.col("a.LOWEST_LEVEL_PORTFOLIO_NAME")) == su.normalize_text( F.col("b.LOWEST_LEVEL_PORTFOLIO_NAME")) )
                        & ( F.col("a.ISSUE_CURRENCY") == F.col("b.ISSUE_CURRENCY") )
                        & ( F.col("a.SEGMENT_ISSUECURRENCY") == F.col("b.SEGMENT_ISSUECURRENCY") )
                        & ( F.col("a.SCENARIO") == F.col("b.SCENARIO") )
                    ),
                how = "inner"
            )
            .withColumn("AMT", F.col("a.AMOUNT") + F.col("b.AMOUNT"))
            .select("a.SEGMENT", "a.TAM_SEGMENT", "a.LOWEST_LEVEL_PORTFOLIO_NAME", "a.ISSUE_CURRENCY", "a.SEGMENT_ISSUECURRENCY", "a.SCENARIO", F.col("AMT").alias("AMOUNT"))
        )

        total_losses_df = self.cache_df(total_losses_df, "total_losses_df")

        self.write(total_losses_df, self._step_config.step_total_losses.table_name, self._step_config.step_total_losses.ext_table_loc, mode=RDABaseStep.spark_write_mode)
        self._logger.info("Non HA - Total losses calculation completed")
        
        return total_losses_df