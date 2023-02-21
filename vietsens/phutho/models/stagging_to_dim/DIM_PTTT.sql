
{{ config(order_by='()', engine='Join(ANY, LEFT, MaLoaiPTTT)', materialized='incremental') }}
with aaaa as(
--     with Gia_Tri_Ban_Dau as(
-- select toString((farmHash64(upperUTF8('Phẫu thuật đặc biệt')))) as MaLoaiPTTT,(upperUTF8('Phẫu thuật đặc biệt'))   as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Thủ thuật loại 2')))) as MaLoaiPTTT,(upperUTF8('Thủ thuật loại 2'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Thủ thuật loại 3')))) as MaLoaiPTTT,(upperUTF8('Thủ thuật loại 3'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Phẫu thuật loại 3')))) as MaLoaiPTTT,(upperUTF8('Phẫu thuật loại 3'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Thủ thuật loại 1')))) as MaLoaiPTTT,(upperUTF8('Thủ thuật loại 1'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Phẫu thuật loại 2')))) as MaLoaiPTTT,(upperUTF8('Phẫu thuật loại 2'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Thủ thuật đặc biệt')))) as MaLoaiPTTT,(upperUTF8('Thủ thuật đặc biệt'))    as LoaiPTTT
-- union all 
-- select toString((farmHash64(upperUTF8('Phẫu thuật loại 1')))) as MaLoaiPTTT,(upperUTF8('Phẫu thuật loại 1'))    as LoaiPTT
--     ) 
with Gia_Tri_Ban_Dau as (select toString(farmHash64(LoaiPTTT)) as MaLoaiPTTT, LoaiPTTT from {{ref('REF_PTTT')}})
select * from Gia_Tri_Ban_Dau
union distinct
select distinct toString(farmHash64(LoaiPTTT_BF)) as MaLoaiPTTT, LoaiPTTT_BF as LoaiPTTT from 
(select LoaiPTTT_BF,LoaiPTTT,MaLoaiPTTT,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiPTTT_BF),upperUTF8(LoaiPTTT)) as TyLe,
ROW_NUMBER() over(partition by LoaiPTTT_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiPTTT_BF),upperUTF8(LoaiPTTT))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.LoaiPTTT) ,'KHÁC') as LoaiPTTT_BF, 
            Gia_Tri_Ban_Dau.LoaiPTTT  as LoaiPTTT ,Gia_Tri_Ban_Dau.MaLoaiPTTT  as MaLoaiPTTT
            from STAGING_ChiDinhDichVu as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.3
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where MaLoaiPTTT not in (select distinct MaLoaiPTTT from {{this}} )
{% endif %}