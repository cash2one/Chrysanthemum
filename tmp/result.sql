-- 商场 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-BUSSINESS-NONMAP-%');

-- 医院 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-HOSPITAL-%' or cp.ref_cptype_code like 'CP-PRIVATE-HOSPITAL%');

-- 4s 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-4S%');

-- 家电 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-JIADIAN%');

-- 家居建材 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-JIAJU-JIANCAI-JIADIAN%');

-- 售楼处 通过审核
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-HOUSESELL%');

-- 商场 通过审核 店内
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-BUSSINESS-NONMAP-INDOOR');


-- 商场 通过审核 店外
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-BUSSINESS-NONMAP-OUTDOOR');

-- 医院 通过审核 科室
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-HOSPITAL-ROOM' or cp.ref_cptype_code = 'CP-PRIVATE-HOSPITAL-ROOM');

-- 4s 通过审核 展车厅
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-4S-SHOWROOM%');

-- 家电 通过审核 销售区
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-JIADIAN-SELL');

-- 家居建材 通过审核 店内
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL');

-- 家居建材 通过审核 店外
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT');

-- 商场 通过审核 已清洗
select count(distinct(ref_cp_code)) from tbl_cp_tag where ref_cp_code in (select cp.ref_cp_code from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-BUSSINESS-NONMAP-%'));

-- 医院 通过审核 已清洗
select count(distinct(ref_cp_code)) from tbl_cp_tag where ref_cp_code in (select cp.ref_cp_code from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-HOSPITAL%' or cp.ref_cptype_code like 'CP-PRIVATE-HOSPITAL%'));

-- 4s 通过审核 已清洗
select count(distinct(ref_cp_code)) from tbl_cp_tag where ref_cp_code in (select cp.ref_cp_code from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-4S-SHOWROOM%'));

-- 家电 通过审核 已清洗
select count(distinct(ref_cp_code)) from tbl_cp_tag where ref_cp_code in (select cp.ref_cp_code from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-JIADIAN%'));

-- 家居建材 通过审核 已清洗
select count(distinct(ref_cp_code)) from tbl_cp_tag where ref_cp_code in (select cp.ref_cp_code from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-JIAJU-JIANCAI-JIADIAN%'));

-- 商场 通过审核 未清洗
select count(cp.ref_cp_code) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-BUSSINESS-NONMAP-%') and cp.ref_cp_code not in (select ref_cp_code from tbl_cp_tag);

-- 商场 通过审核 未清洗 去重
select count(distinct(cp.ref_cp_code)) from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067) and (cp.ref_cptype_code like 'CP-BUSSINESS-NONMAP-%') and cp.ref_cp_code not in (select ref_cp_code from tbl_cp_tag);
