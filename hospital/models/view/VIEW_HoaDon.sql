
{{ config(materialized='view') }}
with CTE as 
(
SELECT
    Fact._id AS _id

    ,Fact.MaCSKCB AS MaCSKCB
    ,joinGet({{ref('DIM_CSKCB')}},'TenCSKCB',MaCSKCB) as TenCSKCB1
    ,Fact.SoTien AS SoTien

    ,joinGet ({{ref('DIM_Ngay')}},'Ngay',Fact.NgayThu) as NgayThu1
    ,joinGet ({{ref('DIM_Ngay')}},'Ngay',coalesce(toInt64(substring(toString(NgayDongBo),1,8)),0)) as NgayDongBo1
FROM {{ref('FACT_HoaDon')}} as Fact FINAL)
select _id, MaCSKCB,TenCSKCB1 as TenCSKCB, NgayThu1 as NgayThu,
 NgayDongBo1 as NgayDongBo from CTE