
{{ config(order_by='MaDanToc', engine='Join(ANY, LEFT, MaDanToc)', materialized='table') }}
with aaaa as(
select '1' as MaDanToc,'Kinh' as DanToc
union distinct
select '2' as MaDanToc,'Tày' as DanToc
union distinct
select '3' as MaDanToc,'Thái' as DanToc
union distinct
select '4' as MaDanToc,'Hoa' as DanToc
union distinct
select '5' as MaDanToc,'Khơ-me' as DanToc
union distinct
select '6' as MaDanToc,'Mường' as DanToc
union distinct
select '7' as MaDanToc,'Nùng' as DanToc
union distinct
select '8' as MaDanToc,'HMông' as DanToc
union distinct
select '9' as MaDanToc,'Dao' as DanToc
union distinct
select '10' as MaDanToc,'Gia-rai' as DanToc
union distinct
select '11' as MaDanToc,'Ngái' as DanToc
union distinct
select '12' as MaDanToc,'Ê-đê' as DanToc
union distinct
select '13' as MaDanToc,'Ba na' as DanToc
union distinct
select '14' as MaDanToc,'Xơ-Đăng' as DanToc
union distinct
select '15' as MaDanToc,'Sán Chay' as DanToc
union distinct
select '16' as MaDanToc,'Cơ-ho' as DanToc
union distinct
select '17' as MaDanToc,'Chăm' as DanToc
union distinct
select '18' as MaDanToc,'Sán Dìu' as DanToc
union distinct
select '19' as MaDanToc,'Hrê' as DanToc
union distinct
select '20' as MaDanToc,'Mnông' as DanToc
union distinct
select '21' as MaDanToc,'Ra-glai' as DanToc
union distinct
select '22' as MaDanToc,'Xtiêng' as DanToc
union distinct
select '23' as MaDanToc,'Bru-Vân Kiều' as DanToc
union distinct
select '24' as MaDanToc,'Thổ' as DanToc
union distinct
select '25' as MaDanToc,'Giáy' as DanToc
union distinct
select '26' as MaDanToc,'Cơ-tu' as DanToc
union distinct
select '27' as MaDanToc,'Gié Triêng' as DanToc
union distinct
select '28' as MaDanToc,'Mạ' as DanToc
union distinct
select '29' as MaDanToc,'Khơ-mú' as DanToc
union distinct
select '30' as MaDanToc,'Co' as DanToc
union distinct
select '31' as MaDanToc,'Tà-ôi' as DanToc
union distinct
select '32' as MaDanToc,'Chơ-ro' as DanToc
union distinct
select '33' as MaDanToc,'Kháng' as DanToc
union distinct
select '34' as MaDanToc,'Xinh-mun' as DanToc
union distinct
select '35' as MaDanToc,'Hà Nhì' as DanToc
union distinct
select '36' as MaDanToc,'Chu ru' as DanToc
union distinct
select '37' as MaDanToc,'Lào' as DanToc
union distinct
select '38' as MaDanToc,'La Chí' as DanToc
union distinct
select '39' as MaDanToc,'La Ha' as DanToc
union distinct
select '40' as MaDanToc,'Phù Lá' as DanToc
union distinct
select '41' as MaDanToc,'La Hủ' as DanToc
union distinct
select '42' as MaDanToc,'Lự' as DanToc
union distinct
select '43' as MaDanToc,'Lô Lô' as DanToc
union distinct
select '44' as MaDanToc,'Chứt' as DanToc
union distinct
select '45' as MaDanToc,'Mảng' as DanToc
union distinct
select '46' as MaDanToc,'Pà Thẻn' as DanToc
union distinct
select '47' as MaDanToc,'Co Lao' as DanToc
union distinct
select '48' as MaDanToc,'Cống' as DanToc
union distinct
select '49' as MaDanToc,'Bố Y' as DanToc
union distinct
select '50' as MaDanToc,'Si La' as DanToc
union distinct
select '51' as MaDanToc,'Pu Péo' as DanToc
union distinct
select '52' as MaDanToc,'Brâu' as DanToc
union distinct
select '53' as MaDanToc,'Ơ Đu' as DanToc
union distinct
select '54' as MaDanToc,'Rơ măm' as DanToc
union distinct
select '55' as MaDanToc,'Người nước ngoài' as DanToc
union distinct
select '56' as MaDanToc,'Khác' as DanToc
)
select * from aaaa
