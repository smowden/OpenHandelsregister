# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM offene_register WHERE all_attributes.native_company_number LIKE 'Berlin (Charlottenburg) HRB 200169%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM offene_register WHERE all_attributes.native_company_number LIKE 'Berlin (Charlottenburg)%' ORDER BY size(officers) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM companies WHERE referenceNumber LIKE 'Charlottenburg (Berlin) HRB 200169 B'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM roles_people WHERE referenceNumber LIKE 'Charlottenburg (Berlin) HRB 200169 B'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_stage2 WHERE referenceNumber LIKE 'Charlottenburg (Berlin) HRB 200169 B'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM publications_classified WHERE id = 513062 AND landAbk = 'be'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT size(officers) FROM offene_register WHERE all_attributes.native_company_number LIKE 'Berlin (Charlottenburg) HRB 106191'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM roles_people WHERE referenceNumber LIKE 'Charlottenburg (Berlin) HRB 106191%' ORDER BY lastName, firstName