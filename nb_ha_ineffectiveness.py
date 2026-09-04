# Databricks notebook source
# MAGIC %run ../../common/nb_load_rda_package

# COMMAND ----------

from rda.utils.logger import get_notebook_logger
logger = get_notebook_logger()
logger.info("EAR engine HA Ineffectiveness started...")

# COMMAND ----------

engine_key = "ha_ineffectiveness"
engine_config = env_config.get(f"engine.ear.{engine_key}")

# COMMAND ----------

from rda.engine.ear.ha_ineffectiveness import HAIneffectiveness
ha_ineffectiveness = HAIneffectiveness(env_config, spark, engine_config)

inputs = ha_ineffectiveness.read()

partial_dv01_df = ha_ineffectiveness.calculate_partial_dv01(inputs)
losses_df = ha_ineffectiveness.calculate_losses(inputs, partial_dv01_df)

ha_ineffectiveness.unpersist_all()

# COMMAND ----------

logger.info("EAR engine HA Ineffectiveness completed successfully")
