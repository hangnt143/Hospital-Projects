
{{ config(order_by='(ThongTinDieuTriID)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(
SELECT 
 coalesce(`_id`,'-1')  as `ThongTinDieuTriID` ,
coalesce(    `MaVaoVien` ,'-1') as     `MaVaoVien` ,
coalesce(    `MaBN` ,'-1') as     `MaBN` ,
coalesce(    `Tuoi` ,-1) as     `Tuoi` ,
TrangThai ,
MAPPING_BenhAn.`MaLoaiBenhAn`  as     `MaLoaiBenhAn` ,
MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien` as     `MaDoiTuongVaoVien` ,
coalesce(    `MaCSKCB` ,'-1') as     `MaCSKCB` ,
MAPPING_Khoa.MaKhoa as MaKhoaVaoVien,
coalesce(    `MaICDVaoVien` ,'-1') as     `MaICDVaoVien` ,
coalesce(    `SoVaoVien` ,'-1') as     `SoVaoVien` ,
coalesce(   toInt64(substring(toString(NgayBatDauDieuTriINH),1,8)),0) as `NgayBatDauDieuTriINH`,
coalesce(   toInt64(substring(toString(NgayBatDauDieuTriLao),1,8)),0) as `NgayBatDauDieuTriLao`,
coalesce(   toInt64(substring(toString(NgayBatDauDieuTriARV),1,8)),0) as `NgayBatDauDieuTriARV`,
coalesce(   toInt64(substring(toString(NgayBatDauDieuTriThuocARV),1,8)),0) as `NgayBatDauDieuTriThuocARV`,
coalesce(   toInt64(substring(toString(NgayVaoVien),1,8)),0) as `NgayVaoVien`,
coalesce(   toInt64(substring(toString(NgayRaVien),1,8)),0) as `NgayRaVien`,
coalesce(    `BenhChinhVaoVien` ,'-1') as     `BenhChinhVaoVien` ,
coalesce(   toInt64(substring(toString(NgayBong),1,8)),0) as `NgayBong`,
coalesce(    `MaICDRaVien` ,'-1') as     `MaICDRaVien` ,
coalesce(    `BenhChinhRaVien` ,'-1') as     `BenhChinhRaVien` ,
coalesce(    toString(`MoCapCuu` ),'-1') as     `MoCapCuu` ,
coalesce(   toInt64(substring(toString(NgayHeThong),1,8)),0) as `NgayHeThong`,
coalesce(   toInt64(substring(toString(NgayTao),1,8)),0) as `NgayTao`,
coalesce(   toInt64(substring(toString(NgayLaySTT),1,8)),0) as `NgayLaySTT`,
coalesce(   toInt64(substring(toString(NgayDongBenhAn),1,8)),0) as `NgayDongBenhAn`,
coalesce(   toInt64(substring(toString(NgayThanhToanVienPhi),1,8)),0) as `NgayThanhToanVienPhi`,
coalesce(   toInt64(substring(toString(NgayPhatThuoc),1,8)),0) as `NgayPhatThuoc`,
coalesce(   toInt64(substring(toString(NgayHenKhamKeTiep),1,8)),0) as `NgayHenKhamKeTiep`,
coalesce(    `HinhThucRaVien` ,'-1') as     `HinhThucRaVien` ,
-- coalesce(   toInt64(substring(toString(NgayDongBo),1,8)),0) as `NgayDongBo`,
coalesce("NgayDongBo",0) as `NgayDongBo`, 
TimeCreate,
COALESCE (case
    when s.NgayVaoVien is null then 0
    when s.NgayRaVien is null  then 0
    when  DATE_DIFF('minute',toDateTimeFromInt(s.NgayVaoVien),toDateTimeFromInt(s.NgayRaVien))< 0 then -1
    else DATE_DIFF('minute',toDateTimeFromInt(s.NgayVaoVien),toDateTimeFromInt(s.NgayRaVien))
    end, 0) as SoPhutTongThoiGianKham
    from STAGING_ThongTinDieuTri as s
left join {{ref('MAPPING_DoiTuongVaoVien')}} on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.DoiTuongVaoVien,'KHÁC')
left join {{ref('MAPPING_BenhAn')}} on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
left join {{ref('MAPPING_Khoa')}}  on MAPPING_Khoa.TenKhoa_BF =  COALESCE (s.KhoaVaoVien,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}