-- Returns 1 if each element of the array is greater than the previous one, 0 otherwise.

WITH [ 2, 3, 5, 2, 3, 55, 33, 4, 1, 2, 1, 2 ] AS data
SELECT arrayFold(
    (acc, x) -> arrayPushBack(acc, (x, if(x > acc[-1].1 AND acc[-1].2 = 1, 1, 0))),
    arrayPopFront(data),
    [(CAST(data[1], 'Int64'), 1::UInt8)]
)[-1].2 AS ordered;


CREATE OR REPLACE FUNCTION isMonotonous AS (x) ->
    arrayFold(
        (acc, x) -> ( x::Int32, acc.2 AND x >= acc.1 ),
        arrayPopFront(x),
        (x[1]::Int32, 1::UInt8)
    ).2;

SELECT isMonotonous([ 1, 4, 6, 40 ]);

CREATE OR REPLACE FUNCTION eliminateBacksteps AS (x) ->
    arrayFold(
        (acc, x) -> ( if(x >= acc.1[-1], arrayPushBack(acc.1, x), acc.1), x >= acc.1[-1] ),
        arrayPopFront(x),
        ([ x[1]::Int32 ], 1::UInt8)
    );

SELECT eliminateBacksteps([ 3, 4, 6, 4, 7, 2, 4, 10 ]);
