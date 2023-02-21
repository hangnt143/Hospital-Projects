
{{ config(order_by='(_id)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(   

SELECT
    COALESCE (`_id`,'-1') as _id ,
    coalesce(`ThongTinHoaDonId`,'-1') as `HoaDonId` ,
    toString(farmHash64(ifNull("MaCSKCB",'-1'),ifNull("MaDichVu",'-1'))) as "MaDichVuID",
    coalesce(`MaDichVu`,'-1') as `MaDichVu` ,
    KTT.MaKhoa as MaKhoaThuTien,
    KCD.MaKhoa as MaKhoaChiDinh,
    coalesce(`SoLuong`,0) as `SoLuong` ,
    coalesce(`GiaDichVu`,0) as `GiaDichVu` ,
    coalesce(`GiaBHYT`,0) as `GiaBHYT` ,
    coalesce(`ThanhTien`,0) as `ThanhTien` ,
    coalesce(`MaKyThuatDungChung`,'-1') as `MaKyThuatDungChung` ,
    coalesce(`BNDCT`,0) as `BNDCT` ,
    coalesce(`BHYTTra`,0) as `BHYTTra` ,
    coalesce(`HaoPhi`,0) as `HaoPhi` ,
    coalesce(toInt64(substring(toString(ThoiGianChiDinh),1,8)),0) as `ThoiGianChiDinh` ,
    coalesce(`BNTra`,0) as `BNTra` ,
    coalesce(toInt64(substring(toString(NgayThu),1,8)),0) as `NgayThu` ,
    -- coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo` ,
    coalesce("NgayDongBo",0) as `NgayDongBo`,
    TimeCreate,
    coalesce(`MaCSKCB`,'-1') as `MaCSKCB` ,
    MAPPING_BenhAn.`MaLoaiBenhAn` as  `MaLoaiBenhAn` ,
    MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien`as "MaDoiTuongThanhToan",
    coalesce(toInt64(substring(toString(NgayHuy),1,8)),0) as `NgayHuy`
    from STAGING_HoaDon_ChiTietHoaDon as s
    left join {{ref('MAPPING_DoiTuongVaoVien')}} on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.TenDoiTuongThanhToan,'KHÁC')
    left join {{ref('MAPPING_BenhAn')}}  on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
    left join {{ref('MAPPING_Khoa')}} as KTT on KTT.TenKhoa_BF =  COALESCE (s.TenKhoaThuTien,'KHÁC')
    left join {{ref('MAPPING_Khoa')}} as KCD on KCD.TenKhoa_BF =  COALESCE (s.TenKhoaChiDinh,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}