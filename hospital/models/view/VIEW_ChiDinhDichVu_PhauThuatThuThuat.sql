
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    Fact._id AS _id

    ,Fact.ThongTinDieuTriID AS ThongTinDieuTriID

    ,Fact.MaCSKCB AS MaCSKCB
    ,joinGet({{ref('DIM_CSKCB')}}, 'TenCSKCB', MaCSKCB) as TenCSKCB1
    ,joinGet({{ref('DIM_Ngay')}}, 'Ngay', ThoiGianPTTT) as ThoiGianPTTT1
    ,joinGet({{ref('DIM_PTTT')}}, 'LoaiPTTT', MaLoaiPTTT) as LoaiPTTT1
FROM {{ref('FACT_ChiDinhDichVu_PhauThuatThuThuat')}} AS Fact FINAL
)
select _id, ThongTinDieuTriID,MaCSKCB, TenCSKCB1 as TenCSKCB, 
ThoiGianPTTT1 as ThoiGianPTTT, LoaiPTTT1 as LoaiPTTT from CTE