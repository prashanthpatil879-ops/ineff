# Databricks notebook source
# MAGIC %run ../../common/nb_load_rda_package

# COMMAND ----------

from rda.utils.logger import get_notebook_logger
logger = get_notebook_logger()
logger.info("EAR engine NonHA started...")

# COMMAND ----------

engine_key = "non_ha"
engine_config = env_config.get(f"engine.ear.{engine_key}")

# COMMAND ----------

from rda.engine.ear.non_ha import NonHA
nonha = NonHA(env_config, spark, engine_config)

inputs = nonha.read()

dv01_posttax_df = nonha.post_tax_calculation(inputs)
amt_with_cl95_df = nonha.calculate_losses(inputs, dv01_posttax_df)
ir_gamma_df = nonha.calculate_ir_gamma(inputs)
losses_with_gamma = nonha.calculate_losses_with_gamma(inputs, ir_gamma_df)
total_losses_df = nonha.calculate_total_losses(inputs, amt_with_cl95_df, losses_with_gamma)

nonha.unpersist_all()

# COMMAND ----------

logger.info("EAR engine NonHA completed successfully")