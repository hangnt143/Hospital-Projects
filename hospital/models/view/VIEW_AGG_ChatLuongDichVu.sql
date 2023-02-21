
{{ config(materialized='view') }}
with CTE as 
(
-- phutho.VIEW_AGG_ChatLuongDichVu source
SELECT
    '01.CHO KHAM' AS TenDichVu,

    ThoiGianTiepDon AS Ngay,

    MaCSKCB,

    sum(SoPhutChoKham) AS TongThoiGian,

    count(MaPhong) AS TongSoCa,

    max(SoPhutChoKham) AS GiaTriLonNhat,

    min(SoPhutChoKham) AS GiaTriNhoNhat
FROM {{ref('VIEW_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong')}} 
WHERE MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006')
GROUP BY
    ThoiGianTiepDon,

    MaCSKCB
UNION ALL
SELECT
    '02.KHÁM BỆNH (THEO PHÒNG)' AS TenDichVu,

    ThoiGianTiepDon AS Ngay,

    MaCSKCB,

    sum(SoPhutKhamBenh) AS TongGiaTri,

    count(MaPhong) AS TongSoCa,

    max(SoPhutKhamBenh) AS GiaTriLonNhat,

    min(SoPhutKhamBenh) AS GiaTriNhoNhat
FROM {{ref('VIEW_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong')}} 
WHERE MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006')
GROUP BY
    ThoiGianTiepDon,

    MaCSKCB
UNION ALL
SELECT
    '03.TỔNG THỜI GIAN KHÁM' AS TenDichVu,

    NgayVaoVien AS Ngay,

    MaCSKCB,

    sum(SoPhutTongThoiGianKham) AS TongGiaTri,

    count(ThongTinDieuTriID) AS TongSoCa,

    max(SoPhutTongThoiGianKham) AS GiaTriLonNhat,

    min(SoPhutTongThoiGianKham) AS GiaTriNhoNhat
FROM {{ref('VIEW_ThongTinDieuTri')}} 
WHERE (MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006')) AND (LoaiBenhAn IN ('KHÁM',
 'NGOẠI TRÚ'))
GROUP BY
    NgayVaoVien,

    MaCSKCB
UNION ALL
SELECT
    '04.CHỜ LẤY MẪU BỆNH PHẨM' AS TenDichVu,

    ThoiGianChiDinh AS Ngay,

    MaCSKCB AS MaCSKCB,

    sum(SoPhutChoLayMau) AS TongGiaTri,

    count(_id) AS TongSoCa,

    max(SoPhutChoLayMau) AS GiaTriLonNhat,

    min(SoPhutChoLayMau) AS GiaTriNhoNhat
FROM {{ref('VIEW_ChiDinhDichVu')}}
WHERE (SoPhutChoLayMau > 0) AND (MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006'))
GROUP BY
    ThoiGianChiDinh,

    MaCSKCB
UNION ALL
SELECT
    '05.XÉT NGHIỆM' AS TenDichVu,

    ThoiGianChiDinh AS Ngay,

    MaCSKCB AS MaCSKCB,

    sum(SoPhutXetNghiem) AS TongGiaTri,

    count(_id) AS TongSoCa,

    max(SoPhutXetNghiem) AS GiaTriLonNhat,

    min(SoPhutXetNghiem) AS GiaTriNhoNhat
FROM {{ref('VIEW_ChiDinhDichVu')}}
WHERE (SoPhutXetNghiem > 0) AND (MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006'))
GROUP BY
    ThoiGianChiDinh,

    MaCSKCB
UNION ALL
SELECT
    multiIf(MaLoaiDichVu = 'CDHA_XQ',
 '06.X QUANG',
 MaLoaiDichVu = 'CDHA_SA',
 '07.SIÊU ÂM',
 MaLoaiDichVu = 'CDHA_CT',
 '08.CHỤP CT',
 MaLoaiDichVu = 'CDHA_MRI',
 '09.CHỤP MRI',
 MaLoaiDichVu = 'CDHA_NS',
 '10.NỘI SOI',
 MaLoaiDichVu = 'TDCN_DTD',
 '11.ĐIỆN TÂM ĐỒ',
 NULL) AS TenDichVu,

    ThoiGianChiDinh AS Ngay,

    MaCSKCB AS MaCSKCB,

    sum(SoPhutXetNghiem) AS TongGiaTri,

    count(_id) AS TongSoCa,

    max(SoPhutXetNghiem) AS GiaTriLonNhat,

    min(SoPhutXetNghiem) AS GiaTriNhoNhat
FROM {{ref('VIEW_ChiDinhDichVu')}}  AS t
WHERE (SoPhutXetNghiem > 0) AND (MaCSKCB IN ('25001',
 '25012',
 '25013',
 '25010',
 '25011',
 '25002',
 '25015',
 '25008',
 '25009',
 '25045',
 '25014',
 '25007',
 '25006')) AND (MaLoaiDichVu IN ('CDHA_XQ',
 'CDHA_SA',
 'CDHA_CT',
 'CDHA_MRI',
 'CDHA_NS',
 'TDCN_DTD'))
GROUP BY
    ThoiGianChiDinh,

    MaCSKCB,

    MaLoaiDichVu)
select *from CTE