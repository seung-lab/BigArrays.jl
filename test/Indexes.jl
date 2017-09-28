using BigArrays.Indexes

using Base.Test

@testset "test index transformation" begin
    str = "2968-3480_1776-2288_16912-17424"
    range = [2969:3480, 1777:2288, 16913:17424]
    @test range == Indexes.string2unit_range( str )
    @test str == Indexes.unit_range2string( range )
end 
