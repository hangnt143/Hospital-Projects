
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    Fact.ThongTinDieuTriID AS ThongTinDieuTriID,

    Fact.Tuoi AS Tuoi,

    Fact.HinhThucRaVien AS HinhThucRaVien,

    joinGet({{ref('DIM_Khoa')}},'TenKhoa',MaKhoaVaoVien) as KhoaVaoVien1,

    Fact.MaLoaiBenhAn AS MaLoaiBenhAn,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn',MaLoaiBenhAn) as LoaiBenhAn1,


    Fact.MaDoiTuongVaoVien AS MaDoiTuongVaoVien,
    joinGet({{ref('DIM_DoiTuongVaoVien')}},'DoiTuongVaoVien',MaDoiTuongVaoVien ) as DoiTuongVaoVien1,

    Fact.MaICDRaVien AS MaICDRaVien,

    left(Fact.MaICDRaVien,3) AS MaBenh,

    Fact.MaBN AS MaBN,

    Fact.MaCSKCB AS MaCSKCB,
    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB', MaCSKCB) as TenCSKCB1,
    joinGet({{ref('DIM_Ngay')}},'Ngay', NgayVaoVien) as NgayVaoVien1,
    joinGet({{ref('DIM_Ngay')}},'Ngay', NgayRaVien) as NgayRaVien1,
    joinGet({{ref('DIM_Ngay')}},'Ngay', NgayTao) as NgayTao1,

    Fact.TrangThai AS TrangThai,

    Fact.SoPhutTongThoiGianKham AS SoPhutTongThoiGianKham
FROM {{ref('FACT_ThongTinDieuTri')}} AS Fact FINAL)
select 
    `ThongTinDieuTriID` ,

    `Tuoi` ,

    `HinhThucRaVien` ,

    `KhoaVaoVien1` as KhoaVaoVien ,

    `MaLoaiBenhAn`  ,

    `LoaiBenhAn1` as LoaiBenhAn ,

    `MaDoiTuongVaoVien` ,

    `DoiTuongVaoVien1` as DoiTuongVaoVien ,

    `MaICDRaVien` ,

    `MaBenh` ,

    `MaBN` ,

    `MaCSKCB` ,

    `TenCSKCB1` as TenCSKCB ,

    `NgayVaoVien1` as NgayVaoVien ,

    `NgayRaVien1` as NgayRaVien ,

    `NgayTao1` as NgayTao ,

    `TrangThai` ,

    `SoPhutTongThoiGianKham` 
 from CTE

 