
{{ config(order_by='MaKhoa', engine='Join(Any, Left,MaKhoa)', materialized='incremental') }}
with aaaa as (
with Gia_Tri_Ban_Dau as (select toString(farmHash64(TenKhoa)) as MaKhoa, TenKhoa from {{ref('REF_Khoa')}})
select * from Gia_Tri_Ban_Dau
union distinct
select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa, TenKhoa_BF as TenKhoa from 
(select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.Khoa) ,'KHÁC') as TenKhoa_BF, 
            Gia_Tri_Ban_Dau.TenKhoa  as TenKhoa ,Gia_Tri_Ban_Dau.MaKhoa  as MaKhoa 
            from STAGING_ThongTinDieuTri_ThongTinDieuTriKhoa_ThongTinDieuTriPhong as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.3
 union distinct
 select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa, TenKhoa_BF as TenKhoa from 
(select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.TenKhoaChiDinh) ,'KHÁC') as TenKhoa_BF, 
            Gia_Tri_Ban_Dau.TenKhoa  as TenKhoa ,Gia_Tri_Ban_Dau.MaKhoa  as MaKhoa 
            from STAGING_HoaDon_ChiTietHoaDon as STAG   
            cross join
            Gia_Tri_Ban_Dau )a) b
 where STT = 1 and TyLe > 0.3
 UNION DISTINCT 
 select distinct toString(farmHash64(TenKhoa_BF)) as MaKhoa, TenKhoa_BF as TenKhoa from 
(select TenKhoa_BF,TenKhoa,MaKhoa,ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa)) as TyLe,
ROW_NUMBER() over(partition by TenKhoa_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(TenKhoa_BF),upperUTF8(TenKhoa))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.TenKhoaThuTien) ,'KHÁC') as TenKhoa_BF, 
            Gia_Tri_Ban_Dau.TenKhoa  as TenKhoa ,Gia_Tri_Ban_Dau.MaKhoa  as MaKhoa 
            from STAGING_HoaDon_ChiTietHoaDon as STAG   
            cross join
            Gia_Tri_Ban_Dau )a) b
 where STT = 1 and TyLe > 0.3
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where TenKhoa not in (select distinct TenKhoa from {{this}} )
{% endif %}