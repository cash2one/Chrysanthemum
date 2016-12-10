医院采集点记录数目：189782
```sql
Hive: select count(*) from tbl_cp_prop where tbl_cp_prop.ref_cptype_code like '%HOSPITAL%';
```

根据point_name和prop_value字段匹配到成功打标签的采集点数目：84458
```sql
Hive: select count(*) from tbl_cp_prop where tbl_cp_prop.ref_cptype_code like '%HOSPITAL%'
and tbl_cp_prop.ref_cp_code in (select tbl_cp_tag.ref_cp_code from tbl_cp_tag);
```

选出未被打标签的医院相关的点（特别耗时，因为使用了sub query）：105324
```sql
HIVE: select count(*) from tbl_cp_prop left join tbl_cp_exprop on tbl_cp_prop.ref_cp_code = tbl_cp_exprop.ref_cp_code  where tbl_cp_prop.ref_cptype_code like '%HOSPITAL%' and tbl_cp_prop.ref_cp_code not in (select tbl_cp_tag.ref_cp_code from tbl_cp_tag);

HIVE: INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output'  select tbl_cp_prop.ref_cp_code, tbl_cp_prop.point_name, tbl_cp_exprop.prop_value from tbl_cp_prop left join tbl_cp_exprop on tbl_cp_prop.ref_cp_code = tbl_cp_exprop.ref_cp_code  where tbl_cp_prop.ref_cptype_code like '%HOSPITAL%' and tbl_cp_prop.ref_cp_code not in (select tbl_cp_tag.ref_cp_code from tbl_cp_tag) limit 100;
```
