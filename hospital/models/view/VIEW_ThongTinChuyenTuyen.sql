
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    Fact.ThongTinDieuTriID AS ThongTinDieuTriID,

    Fact.HinhThucChuyenVienID AS HinhThucChuyenVienID,

    Fact.HinhThucChuyenVien AS HinhThucChuyenVien,

    joinGet ({{ref('DIM_Ngay')}},'Ngay', ThoiGianChuyenVien) as ThoiGianChuyenVien1,

    Fact.MaCSKCB AS MaCSKCB,
    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB', MaCSKCB) as TenCSKCB1,

    Fact.MaDoiTuongVaoVien AS MaDoiTuongVaoVien,

    joinGet({{ref('DIM_DoiTuongVaoVien')}}, 'DoiTuongVaoVien', MaDoiTuongVaoVien) as DoiTuongVaoVien1,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn', MaBenhAn) as LoaiBenhAn1,

    Fact.isDeleted AS isDeleted
FROM {{ref('FACT_ThongTinChuyenTuyen')}} AS Fact FINAL)
select `ThongTinDieuTriID` ,

    `HinhThucChuyenVienID` ,

    `HinhThucChuyenVien` ,

    `MaCSKCB` as MaCSKCB ,

    `TenCSKCB1` as TenCSKCB ,

    `DoiTuongVaoVien1` as DoiTuongVaoVien ,

    `LoaiBenhAn1` as LoaiBenhAn ,
    ThoiGianChuyenVien1 as ThoiGianChuyenVien,
    `isDeleted`   from CTE