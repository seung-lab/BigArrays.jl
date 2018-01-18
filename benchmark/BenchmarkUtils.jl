module BenchmarkUtils

export save, cutout, test

function save(ba::BigArray, img)   
    sz = size(img)                 
    t1 = time()                    
    ba[1:sz[1], 1:sz[2], 1:sz[3]] = img                                
    time()-t1                      
end                                

function cutout(ba::BigArray, sz)                                                                                           
    t1 = time()                    
    ba[1:sz[1], 1:sz[2], 1:sz[3]]  
    time()-t1                      
end                                

function test(ba::BigArray, img; testTime = 5)                         
    tsList = Vector()              
    tcList = Vector()              
    for i in 1:TEST_NUM            
        push!(tsList, save(ba, img))                                   
        push!(tcList, cutout(ba, size(img)))                           
    end                            
    totalSize = length(img)*sizeof(eltype(img)) / 1000/1000            
    totalSize / median(tsList), totalSize / median(tcList)             
end   

end # module
