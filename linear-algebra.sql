CREATE OR REPLACE FUNCTION myArraySum AS (arr) ->
    arrayFold(
        (acc, x) -> acc + x,
        arr,
        0::Float64
    );

WITH [ 2, 3, 5, 7, 11 ] AS arr
SELECT arr, myArraySum(arr);

CREATE OR REPLACE FUNCTION myDotProduct AS (a1, a2) ->
    arrayFold(
        (acc, x, y) -> acc + x * y,
        a1,
        a2,
        0::Float64
    );

SELECT myDotProduct([2, 3], [4, 3, 5]);

-- define the Levi-Civita symbol
WITH [[[0, 0, 0], [0, 0, 1], [0, -1, 0]], [[0, 0, -1], [0, 0, 0], [1, 0, 0]], [[0, 1, 0], [-1, 0, 0], [0, 0, 0]]] AS eps
SELECT eps[1][3][2];

select range(1, 4);

CREATE OR REPLACE FUNCTION myOuterProduct AS (v1, v2) ->
    arrayMap((x) -> multiply(x, v2), v1);

SELECT myOuterProduct([3, 5], [7, 11]);

-- multiply matrix and vector
CREATE OR REPLACE FUNCTION myMatrixProduct AS (m, v) ->
    arrayMap((x) -> arrayDotProduct(x, v), m);

SELECT myMatrixProduct([[0, 1], [-1, 0]], [7, 11]);

CREATE OR REPLACE FUNCTION myBilinearForm AS (v1, m, v2) ->
    arrayDotProduct(v1, arrayMap((x) -> arrayDotProduct(x, v2), m));

SELECT myBilinearForm([7, 11], [[0, 1], [1, 0]], [7, 11]);

-- define the vector cross product by slicing the Levi-Civita tensor into 3 matrices and compute a bilinear form on each as one result component
CREATE OR REPLACE FUNCTION myCrossProduct AS (v1, v2) ->
    arrayMap((m) -> arrayDotProduct(v1, arrayMap((x) -> arrayDotProduct(x, v2), m)),
        [[[0, 0, 0], [0, 0, 1], [0, -1, 0]], [[0, 0, -1], [0, 0, 0], [1, 0, 0]], [[0, 1, 0], [-1, 0, 0], [0, 0, 0]]]
    );

SELECT myCrossProduct([1, 0, 0], [0.7, 0.7, 0]);
