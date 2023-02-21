{{ config(order_by='(MaLoaiBenhAn_BF)', engine='MergeTree()', materialized='incremental') }}
with aaaa as(
    with DIM as (select * from {{ref('DIM_LoaiBenhAn')}})
    select farmHash64(LoaiBenhAn_BF)as MaLoaiBenhAn_BF,LoaiBenhAn_BF, LoaiBenhAn, MaLoaiBenhAn 
    from (
        select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe, 
        ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(STAG.LoaiBenhAn,'KHÁC') as LoaiBenhAn_BF,DIM.LoaiBenhAn as LoaiBenhAn,DIM.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ChiDinhDichVu as STAG   
            cross join DIM)
           )
	where STT =1 

    union distinct

    select farmHash64(LoaiBenhAn_BF)as MaLoaiBenhAn_BF,LoaiBenhAn_BF, LoaiBenhAn, MaLoaiBenhAn 
    from (
        select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe, 
        ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(STAG.LoaiBenhAn,'KHÁC') as LoaiBenhAn_BF,DIM.LoaiBenhAn as LoaiBenhAn,DIM.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_HoaDon as STAG   
            cross join DIM)
           )
	where STT =1

    union distinct

        select farmHash64(LoaiBenhAn_BF)as MaLoaiBenhAn_BF,LoaiBenhAn_BF, LoaiBenhAn, MaLoaiBenhAn 
    from (
        select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe, 
        ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(STAG.LoaiBenhAn,'KHÁC') as LoaiBenhAn_BF,DIM.LoaiBenhAn as LoaiBenhAn,DIM.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ThongTinChuyenTuyen as STAG   
            cross join DIM)
           )
	where STT =1 

    union distinct

            select farmHash64(LoaiBenhAn_BF)as MaLoaiBenhAn_BF,LoaiBenhAn_BF, LoaiBenhAn, MaLoaiBenhAn 
    from (
        select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe, 
        ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(STAG.LoaiBenhAn,'KHÁC') as LoaiBenhAn_BF,DIM.LoaiBenhAn as LoaiBenhAn,DIM.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ThongTinDieuTri as STAG   
            cross join DIM)
           )
	where STT =1 
    )

select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where LoaiBenhAn_BF not in (select distinct LoaiBenhAn_BF from {{this}} )

{% endif %}