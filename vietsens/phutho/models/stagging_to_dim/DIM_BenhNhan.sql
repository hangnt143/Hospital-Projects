
{{ config(order_by='(_id)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
with Gia_Tri_Raw as(
SELECT DISTINCT ON ("MaBN","MaCSKCB")
toString(farmHash64(ifNull("MaBN",'-1'),ifNull("MaCSKCB",'-1'))) as "_id"
,"MaBN"
,"MaCSKCB"
,"TenBenhNhan"
,"GioiTinh"
,toString(farmHash64("QuocTich")) as "MaQuocTich"
,toString(farmHash64("TenDanToc")) as "MaDanToc"
,toString(farmHash64("TenNgheNghiep")) as "MaNgheNghiep"
,"NamSinh" as NgaySinh
,0 as "SoCMND"
,0 as "NgayCapCMND"
,'' as "NoiCapCMND"
from STAGING_ThongTinDieuTri 

union distinct

SELECT DISTINCT ON ("MaBN","MaCSKCB")
toString(farmHash64(ifNull("MaBN",'-1'),ifNull("MaCSKCB",'-1'))) as "_id"
,"MaBN"
,"MaCSKCB"
,"HoTen" as "TenBenhNhan" 
,"GioiTinh"
,toString(farmHash64("QuocTich")) as "MaQuocTich"
,toString(farmHash64("DanToc")) as "MaDanToc"
,toString(farmHash64("NgheNghiep")) as "MaNgheNghiep"
,"NgaySinh"
,0 as "SoCMND"
,0 as "NgayCapCMND"
,'' as "NoiCapCMND"
from STAGING_ThongTinChuyenTuyen
)
select "_id","MaBN","MaCSKCB","TenBenhNhan","GioiTinh"
,"MaQuocTich"
,"MaDanToc"
,"MaNgheNghiep"
,"NgaySinh"
,"SoCMND","NgayCapCMND","NoiCapCMND" from Gia_Tri_Raw
-- left join với mapping của các bảng CSKCB,QuocTich,DanToc,NgheNghiep 
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where _id not in (select distinct  "_id" from {{this}} )
{% endif %}