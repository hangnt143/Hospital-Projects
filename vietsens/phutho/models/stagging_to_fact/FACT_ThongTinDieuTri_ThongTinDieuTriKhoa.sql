
{{ config(order_by='(ThongTinDieuTriID)', engine='ReplacingMergeTree(NgayDongBo)', materialized='incremental') }}
with aaaa as(

select
    coalesce(_id,'-1') as `ThongTinDieuTriKhoaID` ,
    coalesce(    `ThongTinDieuTriID` ,'-1') as     `ThongTinDieuTriID` ,
    MAPPING_Khoa.MaKhoa as `MaKhoa` ,	
    coalesce(    `Phong` ,'-1') as     `Phong` ,
    MAPPING_BenhAn.`MaLoaiBenhAn` as MaLoaiBenhAn,
    MAPPING_DoiTuongVaoVien.`MaDoiTuongVaoVien` as MaDoiTuongVaoVien ,
    coalesce(    `MaDieuTri` ,'-1') as     `MaDieuTri` ,
    coalesce(    `MaCSKCB` ,'-1') as     `MaCSKCB` ,
    coalesce(    `DoiTuongKCBBHYT` ,'-1') as     `DoiTuongKCBBHYT` ,
    coalesce(    `MaICDVaoKhoa` ,'-1') as     `MaICDVaoKhoa` ,
    coalesce(    `BenhChinhVaoKhoa` ,'-1') as     `BenhChinhVaoKhoa` ,
    coalesce(    `HinhThucVaoVien` ,'-1') as     `HinhThucVaoVien` ,
    coalesce(   toInt64(substring(toString(ThoiGianVaoKhoa),1,8)),0) as `ThoiGianVaoKhoa`,
    coalesce(   toInt64(substring(toString(TuNgay),1,8)),0) as `TuNgay`,
    coalesce(    `YeuCauKham` ,'-1') as     `YeuCauKham` ,
    coalesce(    `ChuyenVienTheoYeuCau` ,-1) as     `ChuyenVienTheoYeuCau` ,
    coalesce(    toString(`TuVong`) ,'-1') as     `TuVong` ,
    coalesce(   toInt64(substring(toString(ThoiGianTuVong),1,8)),0) as `ThoiGianTuVong`,
    coalesce(   toInt64(substring(toString(ThoiGianRaKhoa),1,8)),0) as `ThoiGianRaKhoa`,
    coalesce(   toInt64(substring(toString(ThoiGianChuyenVien),1,8)),0) as `ThoiGianChuyenVien`,
    coalesce(    `BenhChinhRaKhoa` ,'-1') as     `BenhChinhRaKhoa` ,
    coalesce(   toInt64(substring(toString(DenNgay),1,8)),0) as `DenNgay`,
    coalesce("NgayDongBo",0) as `NgayDongBo`  ,
    TimeCreate
    -- coalesce(   toInt64(substring(toString(NgayTao),1,8)),0) as `NgayDongBo`
    from STAGING_ThongTinDieuTri_ThongTinDieuTriKhoa as s
left join {{ref('MAPPING_DoiTuongVaoVien')}}  on MAPPING_DoiTuongVaoVien.DoiTuongVaoVien_BF = COALESCE (s.DoiTuongVaoVien,'KHÁC')
left join {{ref('MAPPING_BenhAn')}}  on MAPPING_BenhAn.LoaiBenhAn_BF =  COALESCE (s.LoaiBenhAn,'KHÁC')
left join {{ref('MAPPING_Khoa')}}  on MAPPING_Khoa.TenKhoa_BF =  COALESCE (s.Khoa,'KHÁC')
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where NgayDongBo > (select max(NgayDongBo)from {{this}})

{% endif %}