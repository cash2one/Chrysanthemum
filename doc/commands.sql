insert overwrite directory '/user/archer/output' select distinct(point_name) from tbl_cp_prop where (tbl_cp_prop.ref_cptype_code = 'CP-BUSSINESS-NONMAP-INDOOR' or tbl_cp_prop.ref_cptype_code = 'CP-BUSSINESS-NONMAP-OUTDOOR') and tbl_cp_prop.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'BV2');

insert overwrite directory '/user/archer/output' select distinct(point_name) from tbl_cp_prop where (tbl_cp_prop.ref_cptype_code = 'CP-HOSPITAL-ROOM' or tbl_cp_prop.ref_cptype_code = 'CP-PRIVATE-HOSPITAL-ROOM') and tbl_cp_prop.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'HV1.1' or tbl_cp_tag_relation.flag = 'PHV1.1' or tbl_cp_tag_relation.flag = 'PHV1.2');

-- super slow even with the map reduce
insert overwrite directory '/user/archer/output' select distinct(prop_value) from tbl_cp_prop join tbl_cp_exprop on tbl_cp_prop.ref_cp_code = tbl_cp_exprop.ref_cp_code where (tbl_cp_prop.ref_cptype_code = 'CP-HOSPITAL-ROOM' or tbl_cp_prop.ref_cptype_code = 'CP-PRIVATE-HOSPITAL-ROOM') and tbl_cp_prop.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'HV1.1' or tbl_cp_tag_relation.flag = 'PHV1.1' or tbl_cp_tag_relation.flag = 'PHV1.2');

insert overwrite directory '/user/archer/output' select distinct(e.prop_value) from tbl_cp_exprop e inner join tbl_cp_prop p on e.ref_cp_code = p.ref_cp_code where (p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL' or p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT') and e.ref_exprop_code = 'EXP-CATEGORY' and p.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'JIAJUV1');

insert overwrite directory '/user/archer/output' select distinct(p.point_name) from tbl_cp_prop p where (p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL' or p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT') and p.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'JIAJUV2' or tbl_cp_tag_relation.flag = 'JIAJUV1');


insert overwrite directory '/user/archer/output' select distinct(e.prop_value) from tbl_cp_exprop e inner join tbl_cp_prop p on e.ref_cp_code = p.ref_cp_code where p.ref_cptype_code = 'CP-JIADIAN-SELL' and e.ref_exprop_code = 'EXP-CATEGORY' and p.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'JIADIANV1');

insert overwrite directory '/user/archer/output' select distinct(p.point_name) from tbl_cp_prop p where p.ref_cptype_code = 'CP-JIADIAN-SELL' and p.ref_cp_code not in (select tbl_cp_tag_relation.ref_cp_code from tbl_cp_tag_relation where tbl_cp_tag_relation.flag = 'JIADIANV1' or tbl_cp_tag_relation.flag = 'JIADIANV2');
