
{{ config(order_by='MaLoaiBenhAn', engine='Join(Any, Left,MaLoaiBenhAn)', materialized='incremental') }}
with aaaa as (
--     with Gia_Tri_Ban_Dau as (
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'NỘI TRÚ' as LoaiBenhAn
-- union all
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'TIẾP ĐÓN' as LoaiBenhAn
-- union all
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'KHÁC' as LoaiBenhAn
-- union all
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'ĐIỀU TRỊ BAN NGÀY' as LoaiBenhAn
-- union all
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'NGOẠI TRÚ' as LoaiBenhAn
-- union all
-- select toString(farmHash64(LoaiBenhAn)) as MaLoaiBenhAn,'KHÁM' as LoaiBenhAn)
with Gia_Tri_Ban_Dau as (select toString(farmHash64(LoaiBenhAn))as MaLoaiBenhAn, LoaiBenhAn from {{ref('REF_LoaiBenhAn')}})
select * from Gia_Tri_Ban_Dau
union distinct
select toString(farmHash64(LoaiBenhAn_BF)) as MaLoaiBenhAn, LoaiBenhAn_BF as LoaiBenhAn from 
(select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe,
ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.LoaiBenhAn),'KHÁC') as LoaiBenhAn_BF, 
            Gia_Tri_Ban_Dau.LoaiBenhAn as LoaiBenhAn,Gia_Tri_Ban_Dau.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ChiDinhDichVu as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
 union distinct
 select toString(farmHash64(LoaiBenhAn_BF)) as MaLoaiBenhAn, LoaiBenhAn_BF as LoaiBenhAn from 
(select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe,
ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.LoaiBenhAn),'KHÁC') as LoaiBenhAn_BF, 
            Gia_Tri_Ban_Dau.LoaiBenhAn as LoaiBenhAn,Gia_Tri_Ban_Dau.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_HoaDon as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
 union distinct
select MaLoaiBenhAn, LoaiBenhAn from 
(select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe,
ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.LoaiBenhAn),'KHÁC') as LoaiBenhAn_BF,Gia_Tri_Ban_Dau.LoaiBenhAn as LoaiBenhAn,Gia_Tri_Ban_Dau.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ThongTinChuyenTuyen as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
  union distinct
select MaLoaiBenhAn, LoaiBenhAn from 
(select LoaiBenhAn_BF,LoaiBenhAn,MaLoaiBenhAn,ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn)) as TyLe,
ROW_NUMBER() over(partition by LoaiBenhAn_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(LoaiBenhAn_BF),upperUTF8(LoaiBenhAn))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.LoaiBenhAn),'KHÁC') as LoaiBenhAn_BF,Gia_Tri_Ban_Dau.LoaiBenhAn as LoaiBenhAn,Gia_Tri_Ban_Dau.MaLoaiBenhAn as MaLoaiBenhAn
            from STAGING_ThongTinDieuTri as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where LoaiBenhAn not in (select distinct LoaiBenhAn from {{this}} )
{% endif %}