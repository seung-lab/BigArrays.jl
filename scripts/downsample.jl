using BigArrays

## start from aligned BigArrays
using BigArrays.AlignedBigArrays
ba = AlignedBigArray("/mnt/data01/datasets/zebrafish/4_aligned/registry.txt");

# img = ba[:,:,1]

# size(ba)
bb = boundingbox(ba)

r = CartesianRange(ba, 1)
@show r
im = ba[r][:,:,1]

using Images
using ImageView
using AxisArrays
using Colors
# im = reinterpret(Colors.Gray, im)
# image = AxisArray(im, :x,:y)
image = colorview(Colors.Gray, im)
ImageView.imshow(image)
# colorview(Colors.Gray, im)


a = rand(3)
b = a + 2
