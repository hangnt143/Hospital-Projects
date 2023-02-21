
{{ config(order_by='(_id)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(
SELECT 

    coalesce(`_id`,'-1') as _id,

    coalesce(`MaCSKCB`,'-1')  as MaCSKCB,

    coalesce(toInt64(substring(toString(NgayThu),1,8)),0) as `NgayThu` ,

    coalesce(`SoTien`,0) as SoTien,

    coalesce(`MienGiam`,0)  as MienGiam,

    coalesce(`LyDoMienGiam`,'-1') as LyDoMienGiam,

    coalesce(`MaHinhThucThanhToan`,-1)  as MaHinhThucThanhToan,
    
    coalesce(`HinhThucThanhToan`,'-1') as HinhThucThanhToan,

    coalesce(`QuyenSo`,'-1')  as QuyenSo,
    
    -- coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo` ,
    coalesce("NgayDongBo",0) as `NgayDongBo`  ,
    TimeCreate,

    
    coalesce(`SoHoaDon`,'-1')  as SoHoaDon,

    coalesce(`MaLoaiPhieuThu`,'-1') as MaLoaiPhieuThu ,
  coalesce(toInt64(substring(toString(NgayHuy),1,8)),0) as `NgayHuy`
    from STAGING_HoaDon 
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}
