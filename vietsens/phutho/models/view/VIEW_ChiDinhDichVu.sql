
{{ config(materialized='view') }}
with CTE as (
SELECT
    Fact._id AS _id,

    Fact.ThongTinDieuTriID AS ThongTinDieuTriID,

    Fact.BHYTTra AS BHYTTra,

    Fact.BNTra AS BNTra,

    Fact.BNDCT AS BNDCT,

    Fact.MaCSKCB AS MaCSKCB,
    joinGet({{ref('DIM_CSKCB')}},'TenCSKCB',MaCSKCB) as TenCSKCB1,
    
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianChiDinh) as ThoiGianChiDinh1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianKetQua) as ThoiGianKetQua1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianBatDau) as ThoiGianBatDau1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianKetThuc) as ThoiGianKetThuc1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',ThoiGianLayMau) as ThoiGianLayMau1,
    joinGet({{ref('DIM_Ngay')}},'Ngay',coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0)) as NgayDongBo1,


    Fact.GiaDichVu AS GiaDichVu,


    Fact.ThanhTien AS ThanhTien,

    joinGet({{ref('DIM_LoaiBenhAn')}},'LoaiBenhAn',MaLoaiBenhAn) as LoaiBenhAn1,

    joinGet({{ref('DIM_Khoa')}},'TenKhoa',MaKhoaChiDinh) as TenKhoaChiDinh1,

    Fact.LoaiDonThuoc AS LoaiDonThuoc,
    Fact.MaDichVu,
    Dim.MaLoaiDichVu as MaLoaiDichVu,
    Dim.TenLoaiDichVu as TenLoaiDichVu,
    Dim.MaNhomDichVu as MaNhomDichVu,
    Dim.TenNhomDichVu as TenNhomDichVu,

    -- joinGet({{ref('DIM_DichVu')}},'MaLoaiDichVu',MaDichVu) as MaLoaiDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'TenLoaiDichVu',MaDichVu) as TenLoaiDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'MaNhomDichVu',MaDichVu) as MaNhomDichVu1,
    -- joinGet({{ref('DIM_DichVu')}},'TenNhomDichVu',MaDichVu) as TenNhomDichVu1,

    joinGet({{ref('DIM_DoiTuongVaoVien')}},'DoiTuongVaoVien', MaDoiTuongThanhToan) as DoiTuongThanhToan1,

    Fact.TrangThai,

    Fact.SoPhutChoLayMau AS SoPhutChoLayMau,

    Fact.SoPhutXetNghiem AS SoPhutXetNghiem,

    Fact.SoPhutChoKetQua AS SoPhutChoKetQua
FROM {{ref('FACT_ChiDinhDichVu')}} AS Fact FINAL
LEFT JOIN {{ref('DIM_DichVu')}} as Dim on Dim.MaDichVuID= Fact.MaDichVuID
) 
select     
    `_id` ,

    `ThongTinDieuTriID` ,

    `BHYTTra` ,

    `BNTra` ,

    `BNDCT` ,

    `MaCSKCB` ,
    TenCSKCB1 as TenCSKCB,

    `ThoiGianChiDinh1` as ThoiGianChiDinh,

    `ThoiGianBatDau1` as ThoiGianBatDau ,

    `ThoiGianKetQua1` as ThoiGianKetQua ,

    `ThoiGianKetThuc1` as ThoiGianKetThuc ,

    `ThoiGianLayMau1` as ThoiGianLayMau ,

    `GiaDichVu` ,

    `NgayDongBo1` as NgayDongBo ,

    `ThanhTien` ,

    `LoaiBenhAn1` as LoaiBenhAn ,

    `TenKhoaChiDinh1` as TenKhoaChiDinh ,

    `LoaiDonThuoc` ,
    MaDichVu,

    MaLoaiDichVu ,

   TenLoaiDichVu ,

   MaNhomDichVu,

    TenNhomDichVu ,
    DoiTuongThanhToan1 as DoiTuongThanhToan,

    `TrangThai` ,

    `SoPhutChoLayMau` ,

    `SoPhutXetNghiem` ,

    `SoPhutChoKetQua`   
    from CTE