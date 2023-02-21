
{{ config(order_by='MaDoiTuongVaoVien', engine='Join(ANY, LEFT, MaDoiTuongVaoVien)', materialized='table') }}
with aaaa as(
     -- with Gia_Tri_Ban_Dau as(
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien , 'BHYT+YÊU CẦU' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'VIỆN PHÍ' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'YÊU CẦU' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'BHYT' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'DỊCH VỤ' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'KHÁC' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'HAO PHÍ KHÁC' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien, 'HAO PHÍ PTTT' as DoiTuongVaoVien 
-- union all
-- select toString(farmHash64(DoiTuongVaoVien)) as MaDoiTuongVaoVien ,'THANH TOÁN KHÁC' as DoiTuongVaoVien

-- )
with Gia_Tri_Ban_Dau as (select (toString(farmHash64(DoiTuongVaoVien))) as MaDoiTuongVaoVien, DoiTuongVaoVien from {{ref('REF_DoiTuongVaoVien')}})
select * from Gia_Tri_Ban_Dau
union all 
select toString(farmHash64(DoiTuongVaoVien_BF)) as MaDoiTuongVaoVien, DoiTuongVaoVien_BF as DoiTuongVaoVien from 
(select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe,
ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.TenDoiTuongThanhToan),'KHÁC') as DoiTuongVaoVien_BF, 
            Gia_Tri_Ban_Dau.DoiTuongVaoVien as DoiTuongVaoVien,Gia_Tri_Ban_Dau.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ChiDinhDichVu as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
 union all 
select toString(farmHash64(DoiTuongVaoVien_BF)) as MaDoiTuongVaoVien, DoiTuongVaoVien_BF as DoiTuongVaoVien from 
(select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe,
ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.TenDoiTuongThanhToan),'KHÁC') as DoiTuongVaoVien_BF, 
            Gia_Tri_Ban_Dau.DoiTuongVaoVien as DoiTuongVaoVien,Gia_Tri_Ban_Dau.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_HoaDon_ChiTietHoaDon as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4

union all 
 select toString(farmHash64(DoiTuongVaoVien_BF)) as MaDoiTuongVaoVien, DoiTuongVaoVien_BF as DoiTuongVaoVien from 
(select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe,
ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.DoiTuongBenhNhan),'KHÁC') as DoiTuongVaoVien_BF, 
            Gia_Tri_Ban_Dau.DoiTuongVaoVien as DoiTuongVaoVien,Gia_Tri_Ban_Dau.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ThongTinChuyenTuyen as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
 union all 
select toString(farmHash64(DoiTuongVaoVien_BF)) as MaDoiTuongVaoVien, DoiTuongVaoVien_BF as DoiTuongVaoVien from 
(select DoiTuongVaoVien_BF,DoiTuongVaoVien,MaDoiTuongVaoVien,ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien)) as TyLe,
ROW_NUMBER() over(partition by DoiTuongVaoVien_BF order by ngramDistanceCaseInsensitiveUTF8(upperUTF8(DoiTuongVaoVien_BF),upperUTF8(DoiTuongVaoVien))) as STT
            from ( 
            select distinct coalesce(upperUTF8(STAG.DoiTuongVaoVien),'KHÁC') as DoiTuongVaoVien_BF, 
            Gia_Tri_Ban_Dau.DoiTuongVaoVien as DoiTuongVaoVien,Gia_Tri_Ban_Dau.MaDoiTuongVaoVien as MaDoiTuongVaoVien
            from STAGING_ThongTinDieuTri as STAG   
            cross join
            Gia_Tri_Ban_Dau)a) b
 where STT = 1 and TyLe > 0.4
)
select * from aaaa
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where DoiTuongVaoVien not in (select distinct DoiTuongVaoVienfrom {{this}} )

{% endif %}

