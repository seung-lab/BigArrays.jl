a = rand(1000, 1000, 100)

include("../backends/h5s.jl")

# use default constructor
ba = H5sBigArray()

ba[4001:5000, 4001:5000, 501:600] = a

b = ba[4001:5000, 4001:5000, 501:600]

info("maximum difference: $(maximum(abs(a-b)))")
info("----------end-----------")
