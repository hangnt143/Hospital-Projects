
{{ config(order_by='(MaDichVu)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
-- select coalesce(MaDichVu,'KHÁC') as MaDichVu,"TenDichVu","MaLoaiDichVu"
-- ,"TenLoaiDichVu","MaNhomDichVu","TenNhomDichVu"
--  from (
-- SELECT DISTINCT ON ("MaDichVu")
-- "MaDichVu"
-- ,"TenDichVu","MaLoaiDichVu"
-- ,"TenLoaiDichVu","MaNhomDichVu","TenNhomDichVu"
-- from STAGING_HoaDon_ChiTietHoaDon 
-- )

-- union distinct

SELECT DISTINCT ON ("MaDichVu","MaCSKCB")
toString(farmHash64(ifNull("MaCSKCB",'-1'),ifNull("MaDichVu",'-1'))) as "MaDichVuID"
, coalesce(`MaCSKCB`,'-1')  as MaCSKCB
, coalesce(`MaDichVu`,'-1') as  MaDichVu
,"TenDichVu","MaLoaiDichVu"
,"TenLoaiDichVu","MaNhomDichVu","TenNhomDichVu"
from STAGING_ChiDinhDichVu
 )
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where MaDichVuID not in (select distinct MaDichVuID from {{this}} )

{% endif %}