
{{ config(order_by='(_id)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(   

SELECT 

    coalesce(`_id`,'-1') as _id,

    coalesce(`ThongTinDieuTriID`,'-1') as ThongTinDieuTriID,

    MAPPING_PTTT.LoaiPTTT as LoaiPTTT,

    toString(farmHash64(coalesce("TenKhoaChiDinh",'KHÁC'))) as `MaKhoaChiDinh` ,

   MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien` as MaDoiTuongThanhToan,
   coalesce(`TenDoiTuongThanhToan`,'Khác') as `DoiTuongThanhToan`,
   MAPPING_BenhAn.MaLoaiBenhAn as MaLoaiBenhAn,
    toString(farmHash64(ifNull("MaCSKCB",'-1'),ifNull("MaDichVu",'-1'))) as "MaDichVuID",

    coalesce(`MaDichVu`,'-1') as `MaDichVu` ,
   

    coalesce(`MaCSKCB`,'-1') as MaCSKCB,

    coalesce(`MaBacSiChiDinh`,'-1') as MaBacSiChiDinh,

    coalesce(`TrangThai`,'KHÁC')as `TrangThai` ,

    -- coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo` ,
    coalesce("NgayDongBo",0) as `NgayDongBo` ,

    TimeCreate,

    coalesce(toInt64(substring(toString(ThoiGianChiDinh),1,8)),0) as `ThoiGianChiDinh` ,

    coalesce(toInt64(substring(toString(NgayDuyetBHYT),1,8)),0) as `NgayDuyetBHYT`,

    coalesce(toInt64(substring(toString(ThoiGianLayMau),1,8)),0) as `ThoiGianLayMau` ,

    coalesce(toInt64(substring(toString(ThoiGianKetQua),1,8)),0) as `ThoiGianKetQua` ,

    coalesce(toInt64(substring(toString(NgayDuyetKeToan),1,8)),0) as `NgayDuyetKeToan` ,

    coalesce(`SoTheBHYT`,'-1') as SoTheBHYT,

    coalesce(`LoaiPhieu`,'-1') as LoaiPhieu,

    coalesce(`MaKyThuatDungChung`,'-1') as MaKyThuatDungChung,

    coalesce(`DVT`,'-1') as DVT,

    coalesce(`MaHoatChat`,'-1') as MaHoatChat,

    coalesce(`TenHoatChat`,'-1') as TenHoatChat,

    coalesce(`MaATC`,'-1')  as MaATC,

    coalesce(`BHYTTra`,0) as BHYTTra,

    coalesce(`BNDCT`,0) as BNDCT,

    coalesce(`BNTra`,0) as BNTra,

    coalesce(`GiaBHYT`,0) as GiaBHYT,

    coalesce(`GiaDichVu`,0) as GiaDichVu,

    coalesce(`SoLuong`,0) as SoLuong,

    coalesce(`ThanhTien`,0) as ThanhTien,

    coalesce(`TyLeBHYTThanhToan`,0) as TyLeBHYTThanhToan,

    coalesce(toInt64(substring(toString(ThoiGianBatDau),1,8)),0) as `ThoiGianBatDau` ,

    coalesce(toInt64(substring(toString(ThoiGianKetThuc),1,8)),0) as `ThoiGianKetThuc` ,

    coalesce(`LoaiDonThuoc`,-1) as LoaiDonThuoc, 
    
    coalesce(`HamLuong`,'-1') as HamLuong,
    
	COALESCE (
    CASE
    WHEN toDateTimeFromInt(s."ThoiGianChiDinh") > toDateTimeFromInt(s."ThoiGianLayMau") THEN 0
        WHEN s."ThoiGianChiDinh" is null THEN 0
        WHEN s."ThoiGianLayMau"  is null THEN 0
        ELSE date_diff('minute', toDateTimeFromInt(s."ThoiGianChiDinh"), toDateTimeFromInt(s."ThoiGianLayMau"))
        END,0) AS "SoPhutChoLayMau",
    COALESCE (
    CASE
    WHEN toDateTimeFromInt(s."ThoiGianLayMau") > toDateTimeFromInt(s."ThoiGianKetQua") THEN 0
    WHEN toDateTimeFromInt(s."ThoiGianBatDau") > toDateTimeFromInt(s."ThoiGianKetThuc") THEN 0
        WHEN s."ThoiGianLayMau" IS NOT NULL THEN date_diff('minute', toDateTimeFromInt(s."ThoiGianLayMau"),toDateTimeFromInt(s."ThoiGianKetQua"))
ELSE date_diff('minute', toDateTimeFromInt(s."ThoiGianBatDau"),toDateTimeFromInt(s."ThoiGianKetThuc"))
        END,0) AS "SoPhutXetNghiem",
        
   COALESCE (
    CASE
    WHEN toDateTimeFromInt(s."ThoiGianChiDinh") > toDateTimeFromInt(s."ThoiGianKetQua") THEN 0
        WHEN s."ThoiGianChiDinh" is null THEN 0
        WHEN s."ThoiGianKetQua" is null THEN 0
        ELSE date_diff('minute',toDateTimeFromInt(s."ThoiGianChiDinh"),toDateTimeFromInt(s."ThoiGianKetQua"))
        END,0) AS "SoPhutChoKetQua"
     from STAGING_ChiDinhDichVu as s
    left join {{ref('MAPPING_DoiTuongVaoVien')}}  on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.TenDoiTuongThanhToan,'KHÁC')
    left join {{ref('MAPPING_BenhAn')}} on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
       left join {{ref('MAPPING_PTTT')}} on MAPPING_PTTT.LoaiPTTT_BF = COALESCE (s.LoaiPTTT,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}
