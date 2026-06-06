#print("Hello World!") #ถ้าไม่ให้แสดง Hello World ให้ใส่ hashtag ไว้ข้างหน้า
print("Data Pipeline")

name = "Tanapourn T" #เอาค่าชื่อเราเก็บในกล่อง name
number = 10 #int (integer) จำนวนเต็ม
a = 5.5 #floating ทศนิยม
b = 9
print(name)
print(name, number, a, b)

c = a + b
c = c * 10 #เอาตัวแปรที่ได้จาก a+b มา x 10 
print(c)

print(5 % 4) #modulus คือ ที่ใช้สำหรับหา "เศษเหลือ" จากการหาร
print(5 % 2)

if c > 150: #เงื่อนไข ถ้า c > 150 ให้ c เป็น 100
   c = 100 
   a = 100
   b = 100
else:
    c = 9
    a = 9
    b = 9

print(a, b, c)

l = [1, 2, 3, 4, 5] #loop ใช้ for โจทย์ให้หาเลขที่หาร 2 ลงตัว
for each_item in l: #สำหรับ item ที่อยู่ใน l
    if each_item % 2  == 0: 
        print(each_item)


