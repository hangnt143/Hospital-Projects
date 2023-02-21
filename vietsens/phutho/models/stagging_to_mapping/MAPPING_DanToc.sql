
{{ config(order_by='(DanToc_BF)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
 with DIM as (select * from {{ref('DIM_DanToc')}})
    select farmHash64(DanToc_BF)as MaDanToc_BF,DanToc_BF, DanToc, MaDanToc 
    from (
        select DanToc_BF,DanToc,MaDanToc,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DanToc_BF),upperUTF8(DanToc)) as TyLe, 
        ROW_NUMBER() over(partition by DanToc_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DanToc_BF),upperUTF8(DanToc))) as STT
            from ( 
            select distinct coalesce(STAG.TenDanToc,'KHÁC') as DanToc_BF,DIM.DanToc as DanToc,DIM.MaDanToc as MaDanToc
            from STAGING_ThongTinDieuTri as STAG   
            cross join DIM)
           )
	where STT =1 
    
    union distinct
    select farmHash64(DanToc_BF)as MaDanToc_BF,DanToc_BF, DanToc, MaDanToc 
    from (
        select DanToc_BF,DanToc,MaDanToc,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DanToc_BF),upperUTF8(DanToc)) as TyLe, 
        ROW_NUMBER() over(partition by DanToc_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DanToc_BF),upperUTF8(DanToc))) as STT
            from ( 
            select distinct coalesce(STAG.DanToc,'KHÁC') as DanToc_BF,DIM.DanToc as DanToc,DIM.MaDanToc as MaDanToc
            from STAGING_ThongTinChuyenTuyen as STAG   
            cross join DIM)
           )
	where STT =1 
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where DanToc_BF not in (select distinct DanToc_BF from {{this}} )

{% endif %}