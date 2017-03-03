using BigArrays
using BigArrays.H5sBigArrays

baAff = H5sBigArray("~/seungmount/research/Jingpeng/14_zfish/affinitymap/");

fileNames = keys(baAff)

for fileName in fileNames
    origin = fileName2origin( fileName )
    origin2 =
end
