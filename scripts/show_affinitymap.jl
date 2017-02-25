using S3Dicts
using BigArrays
using ImageView
using Images


d = S3Dict("s3://seunglab/pinky40/affinitymap/")
ba = BigArray(d);
dAffRepair = S3Dict("s3://seunglab/pinky40/affinitymap.repair/")
baAffRepair = BigArray(dAffRepair);

d2 = S3Dict("s3://seunglab/pinky40/images/")
ba2 = BigArray(d2);
@show ndims(ba2)
# multi section missing region!
#aff = ba[15361:15872,27649:28160,16401:16516,1:3]
# aff = ba[16385:16896,26113:26624,16433:16448,1:3]

# r = (51265-200+10000:52288+200+10000,52289-200:53312+200,16521+155:16521+170,1);
# r = [100353:100864,22529:23040,16705:16720,1]
r = [30209:30720,30721:31232,16577:16592+112,1:3];
# r = (51201:51712,52225:52736,16577:16592,1);

# show affinity
affx = ba[r...][:,:,:,1];
ImageView.imshow(affx)

affxRepair = baAffRepair[r...][:,:,:,1];
ImageView.imshow(affxRepair)

using Watershed
aff = ba[r...];
# ImageView.imshow(aff[:,:,:,1])
atomicseg(aff);

# show image
img = ba2[r[1:3]...]
using FixedPointNumbers
img = reinterpret(FixedPointNumbers.N0f8, img);
ImageView.imshow(img)

using BigArrays.H5sBigArrays

# zebra fish affinitymap
ba = H5sBigArray("~/seungmount/research/Jingpeng/14_zfish/affinitymap/config.json");

# problematic region: 56769-57792_26305-27328_16617-16744/
# affx = ba[56300:57100,26600:27745,16507:16643,1][:,:,:,1]
# affx = ba[56321:57344,26625:27648,16513:16640,1][:,:,:,1]

affx = ba[56769:57792,26305:27328,16617:16744,1][:,:,:,1]

ZERO = eltype(affx)(0)
Threads.@threads for i in eachindex(affx)
    if affx[i]==NaN
        affx[i] = ZERO
    end
end

using HDF5
rm(expanduser("~/affx.h5"))
HDF5.h5write(expanduser("~/affx.h5"), "affx", affx)

ImageView.imshow(affx)

sleep(200)


using BigArrays.H5sBigArrays
ba3 = H5sBigArray("~/seungmount/research/Jingpeng/14_zfish/affinitymap/");
aff = ba3[49601:50624,28097:29120,17513:17640, 1:3]
using HDF5
h5write(expanduser("~/chunk_49601-50624_28097-29120_17513-17640.aff.h5"), "main", aff)

using EMIRT
sgm = readsgm(expanduser("~/chunk_49601-50624_28097-29120_17513-17640.sgm.h5"))
seg = merge(sgm, 0.3)
using HDF5
h5write(expanduser("~/chunk_49601-50624_28097-29120_17513-17640.seg.h5"), "main", seg)

using BigArrays
using BigArrays.AlignedBigArrays
ba4 = AlignedBigArray("/mnt/data01/datasets/zebrafish/4_aligned/registry.txt");
r = [49601:50624,28097:29120,17513:17640].-16384
img = ba4[r...]
h5write(expanduser("~/chunk_49601-50624_28097-29120_17513-17640.img.h5"), "main", img)
