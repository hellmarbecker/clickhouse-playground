SELECT 
    bitmapToArray(
        bitmapAnd(
            bitmapBuild([1, 2, 3, 4, 5]),
            bitmapBuild([3, 5, 7, 8])
        ) 
    ) AS res;

SELECT 
    finalizeAggregation(
        bitmapAnd(
            bitmapBuild([1, 2, 3, 4, 5]),
            bitmapBuild([3, 5, 7, 8])
        ) 
    ) AS res;

SELECT 
    finalizeAggregation(
            bitmapBuild([1, 5, 1222, 22])
    ) AS res;