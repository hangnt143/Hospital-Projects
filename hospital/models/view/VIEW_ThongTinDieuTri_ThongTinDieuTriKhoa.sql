
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    Fact.ThongTinDieuTriKhoaID AS ThongTinDieuTriKhoaID,

    Fact.ThongTinDieuTriID AS ThongTinDieuTriID,

    -- Fact.Khoa AS Khoa,
    joinGet({{ref('DIM_Khoa')}},'TenKhoa',MaKhoa ) as Khoa1,

    Fact.Phong AS Phong,

    Fact.MaLoaiBenhAn AS MaLoaiBenhAn,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn',MaLoaiBenhAn ) as LoaiBenhAn1,

    Fact.MaDoiTuongVaoVien AS MaDoiTuongVaoVien,

    joinGet({{ref('DIM_DoiTuongVaoVien')}},'DoiTuongVaoVien',MaDoiTuongVaoVien) as DoiTuongVaoVien1,

    Fact.MaDieuTri AS MaDieuTri,

    Fact.MaCSKCB AS MaCSKCB,

    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB', MaCSKCB) as TenCSKCB1,

    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianVaoKhoa) as ThoiGianVaoKhoa1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianRaKhoa) as ThoiGianRaKhoa1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0)) as NgayDongBo1
FROM {{ref('FACT_ThongTinDieuTri_ThongTinDieuTriKhoa')}} AS Fact FINAL)
select 
    
    `ThongTinDieuTriKhoaID` ,

    `ThongTinDieuTriID` ,

    `Khoa1`as Khoa ,

    `Phong` ,

    `MaLoaiBenhAn` ,

    `LoaiBenhAn1` as LoaiBenhAn ,

    `MaDoiTuongVaoVien` ,

    `DoiTuongVaoVien1` as DoiTuongVaoVien ,

    `MaDieuTri` ,

    `MaCSKCB` ,

    `TenCSKCB1` as TenCSKCB ,

    `ThoiGianVaoKhoa1` as ThoiGianVaoKhoa ,

    `ThoiGianRaKhoa1` as ThoiGianRaKhoa ,

    `NgayDongBo1` as NgayDongBo 
 from CTE