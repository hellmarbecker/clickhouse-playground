-- Returns 1 if each element of the array is greater than the previous one, 0 otherwise.

WITH [ 2, 3, 5, 2, 3, 55, 33, 4, 1, 2, 1, 2 ] AS data
SELECT arrayFold(
    (acc, x) -> arrayPushBack(acc, (x, if(x > acc[-1].1 AND acc[-1].2 = 1, 1, 0))),
    arrayPopFront(data),
    [(CAST(data[1], 'Int64'), 1::UInt8)]
)[-1].2 AS ordered;
