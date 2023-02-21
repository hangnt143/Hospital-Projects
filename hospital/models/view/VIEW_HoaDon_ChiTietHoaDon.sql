
{{ config(materialized='view') }}

with CTE as 
(
SELECT
    Fact._id AS _id,

    Fact.HoaDonId AS HoaDonId,

    Fact.BNTra AS BNTra,

    Fact.BHYTTra AS BHYTTra,

    Fact.BNDCT AS BNDCT,

    joinGet({{ref('DIM_Ngay')}},'Ngay',NgayThu) as NgayThu1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianChiDinh) as ThoiGianChiDinh1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0)) as NgayDongBo1,
   

    Fact.MaCSKCB AS MaCSKCB,
    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB',MaCSKCB) as TenCSKCB1,

    Fact.ThanhTien AS ThanhTien,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn',MaLoaiBenhAn) as LoaiBenhAn1,

    Fact. MaDichVu,
    Dim.TenDichVu as TenDichVu,
    Dim.MaLoaiDichVu as MaLoaiDichVu,
    Dim.TenLoaiDichVu as TenLoaiDichVu,
    Dim.MaNhomDichVu as MaNhomDichVu,
    Dim.TenNhomDichVu as TenNhomDichVu,
    -- joinGet({{ref('DIM_DichVu')}},'TenDichVu',MaDichVu) as TenDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'MaNhomDichVu',MaDichVu) as MaNhomDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'TenNhomDichVu',MaDichVu) as TenNhomDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'TenLoaiDichVu',MaDichVu) as TenLoaiDichVu1,

    joinGet({{ref('DIM_DoiTuongVaoVien')}},'DoiTuongVaoVien',MaDoiTuongThanhToan) as DoiTuongThanhToan1

FROM {{ref('FACT_HoaDon_ChiTietHoaDon')}} AS Fact FINAL
LEFT JOIN {{ref('DIM_DichVu')}} as Dim on Dim.MaDichVuID = Fact.MaDichVuID
WHERE Fact.NgayHuy = 0)
select 	`_id` ,

    `HoaDonId` ,

    `BNTra` ,

    `BHYTTra` ,

    `BNDCT` ,

    `NgayThu1` as NgayThu ,

    `ThoiGianChiDinh1` as ThoiGianChiDinh ,

    `NgayDongBo1` as NgayDongBo ,

    `MaCSKCB` ,
    TenCSKCB1 as TenCSKCB,

    `ThanhTien` ,

    `LoaiBenhAn1` as LoaiBenhAn ,
    MaDichVu,
    TenDichVu ,
    MaLoaiDichVu
    TenLoaiDichVu,
    MaNhomDichVu,
    TenNhomDichVu ,
    `DoiTuongThanhToan1` as DoiTuongThanhToan  from CTE