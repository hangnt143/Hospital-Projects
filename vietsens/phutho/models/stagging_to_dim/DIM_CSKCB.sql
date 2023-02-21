
{{ config(order_by='()', engine='Join(Any, Left,MaCSKCB)', materialized='incremental') }}
with aaaa as(
with Gia_Tri_RAW as (
select DISTINCT 
if (MaCSKCB is null,'-1',MaCSKCB ) as MaCSKCB,
 if (MaCSKCB ='-1' ,'Khác',TenCSKCB ) as TenCSKCB,

    '' as `DiaChi` ,

    '' as `MaTinh` ,

    '' as `TenTinh` ,

    '' as `MaHuyen` ,

    '' as`TenHuyen` ,

    '' as `MaXa` ,

   '' as `TenXa`,

 '' as `TuyenBenhVien` 
from STAGING_ChiDinhDichVu

union distinct

select DISTINCT 
if (MaCSKCB is null,'-1',MaCSKCB ) as MaCSKCB,
if (MaCSKCB ='-1' ,'Khác',TenCSKCB ) as TenCSKCB,

    '' as `DiaChi` ,

    '' as `MaTinh` ,

    '' as `TenTinh` ,

    '' as `MaHuyen` ,

    '' as`TenHuyen` ,

    '' as `MaXa` ,

   '' as `TenXa`,

 '' as `TuyenBenhVien` 
from STAGING_ThongTinChuyenTuyen

 union distinct

select DISTINCT 
if (MaCSKCB is null,'-1',MaCSKCB ) as MaCSKCB,
 if (MaCSKCB ='-1' ,'Khác',TenCSKCB ) as TenCSKCB,

    '' as `DiaChi` ,

    '' as `MaTinh` ,

    '' as `TenTinh` ,

    '' as `MaHuyen` ,

    'QuanHuyenCSKCB' as`TenHuyen` ,

    '' as `MaXa` ,

   '' as `TenXa`,

 '' as `TuyenBenhVien` 
from  STAGING_ThongTinDieuTri ) 
select STAG.*
 from Gia_Tri_RAW as STAG left anti join {{ref('REF_CSKCB')}}  as REF
on STAG.MaCSKCB = toString(REF.MaCSKCB)
union DISTINCT 
select 
    toString(`MaCSKCB`) ,
    TenCSKCB,

    `DiaChi` ,

    `MaTinh` ,

    `TenTinh` ,

    `MaHuyen` ,

    `TenHuyen` ,

    `MaXa` ,

    `TenXa` ,

    `TuyenBenhVien` 
    from {{ref('REF_CSKCB')}}
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where MaCSKCB not in (select distinct MaCSKCB from {{this}} )
{% endif %}