{{ config(order_by='(LoaiPTTT_BF)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
    with DIM as (select * from {{ref('DIM_PTTT')}})
select distinct toString(farmHash64(LoaiPTTT_BF)) as MaLoaiPTTT_BF, LoaiPTTT_BF, MaLoaiPTTT, LoaiPTTT from
(select LoaiPTTT_BF,LoaiPTTT,MaLoaiPTTT,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiPTTT_BF),upperUTF8(LoaiPTTT)) as TyLe,
ROW_NUMBER() over(partition by LoaiPTTT_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiPTTT_BF),upperUTF8(LoaiPTTT))) as STT
            from ( 
            select distinct coalesce(STAG.LoaiPTTT ,'KHÁC') as LoaiPTTT_BF, 
            DIM.LoaiPTTT  as LoaiPTTT ,DIM.MaLoaiPTTT  as MaLoaiPTTT
            from STAGING_ChiDinhDichVu as STAG   
            cross join
            DIM)a) b
 where STT = 1
 )
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where LoaiPTTT_BF not in (select distinct LoaiPTTT_BF from {{this}} )

{% endif %}