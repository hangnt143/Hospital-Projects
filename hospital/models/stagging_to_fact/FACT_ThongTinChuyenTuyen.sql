
{{ config(order_by='(ThongTinDieuTriID)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(
SELECT 
coalesce(   `_id`,'-1') as `ThongTinDieuTriID`,
coalesce(    `MaChuyenVien` ,'-1') as     `MaChuyenVien` ,
coalesce(    `MaBN` ,'-1') as     `MaBN` ,
coalesce(    `MaCSKCB` ,'-1') as     `MaCSKCB` ,
coalesce(    `MaDieuTri` ,'-1') as     `MaDieuTri` ,
coalesce(    `Tuyen` ,'-1') as     `Tuyen` ,
coalesce(    `ChanDoan` ,'-1') as     `ChanDoan` ,
coalesce(    `ChanDoanVaoVien` ,'-1') as     `ChanDoanVaoVien` ,
coalesce(    `ChanDoanVaoVien_KemTheo` ,'-1') as     `ChanDoanVaoVien_KemTheo` ,
coalesce(    `DauHieuLamSang` ,'-1') as     `DauHieuLamSang` ,
coalesce(    `HuongDieuTri` ,'-1') as     `HuongDieuTri` ,
coalesce(    `LoaiChuyenVienID` ,-1) as     `LoaiChuyenVienID` ,
coalesce(    `LoaiChuyenVien` ,'-1') as     `LoaiChuyenVien` ,
coalesce(    `LyDoChuyenID` ,-1) as     `LyDoChuyenID` ,
coalesce(    `LyDoChuyen` ,'-1') as     `LyDoChuyen` ,
coalesce(    `KetQuaCanLamSang` ,'-1') as     `KetQuaCanLamSang` ,
coalesce(   toInt64(substring(toString(NgayVaoVien),1,8)),0) as `NgayVaoVien` ,
coalesce(   toInt64(substring(toString(TuNgay),1,8)),0) as `TuNgay` ,
coalesce(    `MaBVChuyenDi` ,'-1') as     `MaBVChuyenDi` ,
coalesce(    `ChanDoanRaVien` ,'-1') as     `ChanDoanRaVien` ,
coalesce(   toInt64(substring(toString(DenNgay),1,8)),0) as `DenNgay` ,
coalesce(    `HinhThucChuyenVienID` ,-1) as     `HinhThucChuyenVienID` ,
coalesce(    `HinhThucChuyenVien` ,'-1') as     `HinhThucChuyenVien` ,
coalesce(   toInt64(substring(toString(NgayRaVien),1,8)),0) as `NgayRaVien` ,
coalesce(   toInt64(substring(toString(NgayTao),1,8)),0) as `NgayTao` ,
coalesce(    `NoiLamViec` ,'-1') as     `NoiLamViec` ,
coalesce(    `PhuongTienVanChuyen` ,'-1') as     `PhuongTienVanChuyen` ,
coalesce(   toInt64(substring(toString(ThoiGianChuyenVien),1,8)),0) as `ThoiGianChuyenVien` ,
coalesce(    `Thuoc` ,'-1') as     `Thuoc` ,
coalesce(    `TinhTrangChuyenDi` ,'-1') as     `TinhTrangChuyenDi` ,
coalesce(    `isDeleted` ,'-1') as     `isDeleted` ,
MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien` as     `MaDoiTuongVaoVien` ,
MAPPING_BenhAn.`MaLoaiBenhAn` as   `MaBenhAn` ,
-- coalesce(  toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo` 
coalesce("NgayDongBo",0) as `NgayDongBo` , 
TimeCreate
from STAGING_ThongTinChuyenTuyen as s
left join {{ref('MAPPING_DoiTuongVaoVien')}}  on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.DoiTuongBenhNhan,'KHÁC')
left join {{ref('MAPPING_BenhAn')}}  on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}
