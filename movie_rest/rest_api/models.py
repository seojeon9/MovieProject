# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models

# SAMPLE
# class CoFacility(models.Model):
#     cf_idx = models.BigAutoField(primary_key=True)
#     loc = models.CharField(max_length=30)
#     fac_popu = models.BigIntegerField(blank=True, null=True)
#     qur_rate = models.BigIntegerField(blank=True, null=True)
#     std_day = models.CharField(max_length=30, blank=True, null=True)

#     class Meta:
#         managed = False
#         db_table = 'co_facility'