
{{ config(order_by='(_id)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(   
SELECT 

    coalesce(`_id`,'-1') as _id,

    coalesce(`ChiDinhDichVuID`,'-1') as ChiDinhDichVuID,
    coalesce(`ThongTinDieuTriID`,'-1')  as ThongTinDieuTriID,
    MAPPING_BenhAn.MaLoaiBenhAn as  MaLoaiBenhAn,

    coalesce(`MaBacSiChiDinh`,'-1')  as MaBacSiChiDinh ,
    toString(farmHash64(ifNull("MaCSKCB",'-1'),ifNull("MaDichVu",'-1'))) as "MaDichVuID",

    coalesce(`MaCSKCB`,'-1')  as MaCSKCB,

    coalesce(`MaDichVu`,'-1') as  MaDichVu,

    MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien` as "MaDoiTuongThanhToan",

    toString(farmHash64(coalesce("TenPhongChiDinh",'KHÁC'))) as `MaPhongChiDinh` ,

    MAPPING_PTTT.MaLoaiPTTT as MaLoaiPTTT,

    -- coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo` ,
    coalesce("NgayDongBo",0) as `NgayDongBo`,
    TimeCreate,

    coalesce(toInt64(substring(toString(ThoiGianPTTT),1,8)),0) as  ThoiGianPTTT,

    coalesce(toInt64(substring(toString(ThoiGianKetThucPTTT),1,8)),0)  as ThoiGianKetThucPTTT,

    coalesce(`MaChanDoanTruocPTTT`,'-1')  as MaChanDoanTruocPTTT,

    coalesce(`ChanDoanTruocPTTT`,'-1')  as ChanDoanTruocPTTT,

    coalesce(`MaChanDoanSauPTTT`,'-1')  as MaChanDoanSauPTTT,

    coalesce(`ChanDoanSauPTTT`,'-1') as ChanDoanSauPTTT ,

    coalesce(`MaTinhHinhPTTT`,'-1')  as MaTinhHinhPTTT,

    coalesce(`MaPhuongPhapPTTT`,'-1')  as MaPhuongPhapPTTT,

    coalesce(`PhuongPhapPTTT`,'-1')  as PhuongPhapPTTT,

    coalesce(`MaPhuongPhapVoCam`,'-1')  as MaPhuongPhapVoCam ,

    coalesce(`MaTuVongTrongPTTT`,'-1')  as MaTuVongTrongPTTT,
    TimeCreate
    from STAGING_ChiDinhDichVu_PhauThuatThuThuat as s

   left join {{ref('MAPPING_DoiTuongVaoVien')}} on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.TenDoiTuongThanhToan,'KHÁC')
   left join {{ref('MAPPING_BenhAn')}} on MAPPING_BenhAn.LoaiBenhAn_BF = COALESCE (s.LoaiBenhAn,'KHÁC')
   left join {{ref('MAPPING_PTTT')}} on MAPPING_PTTT.LoaiPTTT_BF = COALESCE (s.LoaiPTTT,'KHÁC')
)
select * from aaaa 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where  NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}