
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    ThongTinDieuTriPhongID,

    ThongTinDieuTriKhoaID,

    ThongTinDieuTriID,

    MaCSKCB,

    PhongKham,
    Phong,

    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB', MaCSKCB) as TenCSKCB1,

    Fact.MaPhong AS MaPhong,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn', MaLoaiBenhAn) as LoaiBenhAn1,
    
    joinGet({{ref('DIM_DoiTuongVaoVien')}},'DoiTuongVaoVien', MaDoiTuongVaoVien) as DoiTuongVaoVien1,

    joinGet({{ref('DIM_Ngay')}},'Ngay', ThoiGianTiepDon) as ThoiGianTiepDon1,
    joinGet({{ref('DIM_Ngay')}},'Ngay', ThoiGianBatDau) as ThoiGianBatDau1,
    joinGet({{ref('DIM_Ngay')}},'Ngay', ThoiGianKetThuc) as ThoiGianKetThuc1,

    SoPhutChoKham,

    SoPhutKhamBenh
FROM {{ref('FACT_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong')}} AS Fact FINAL)
select 
    `ThongTinDieuTriPhongID` ,

    `ThongTinDieuTriKhoaID` ,

    `ThongTinDieuTriID` ,

    `MaCSKCB` ,

    `MaPhong` ,

    `PhongKham` ,
    Phong,

    `TenCSKCB1` as TenCSKCB ,

    `LoaiBenhAn1` as LoaiBenhAn ,

    `DoiTuongVaoVien1` as DoiTuongVaoVien ,

    `ThoiGianTiepDon1` as ThoiGianTiepDon ,

    `ThoiGianBatDau1` as ThoiGianBatDau ,

    `ThoiGianKetThuc1` as ThoiGianKetThuc ,

    `SoPhutChoKham` ,

    `SoPhutKhamBenh` 
 from CTE