using BigArrays
using GSDicts

d = GSDict("gs://seunglab/pinky40/images/");

ba = BigArray(d);


img = ba[30721:32768,30721:32768,17153:17216];

using FixedPointNumbers
img = reinterpret(FixedPointNumbers.Normed{UInt8,8}, img)

using ImageView

imshow(img)


# f = open("/usr/people/jingpeng/Downloads/pinky40%2Fimages%2F30721-32768_30721-32768_17153-17216")
#
# # readall("/usr/people/jingpeng/Downloads/pinky40%2Fimages%2F30721-32768_30721-32768_17153-17216")
# data = readall(f)
# using FixedPointNumbers
# # img = reinterpret(Normed{UInt8,8}, data)
#
# using Blosc
# data = Blosc.decompress(UInt8, data)
# img = reshape(img, (2048,2048,64))
