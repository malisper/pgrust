WITH RECURSIVE loop AS (
    SELECT '{"stack": [{"type": "expr", "env": {"+": "+", "-": "-", "*": "*", "/": "/", ">": ">", "<": "<", "=": "=", "head": "head", "tail": "tail", "cons": "cons", "empty": "empty"}, "expr": [["lambda", ["f"], ["f", "f", 1, 0, 0]], ["lambda", ["self", "a", "b", "i"], ["if", [">", "i", 10], ["empty"], ["cons", "a", ["self", "self", ["+", "a", "b"], "a", ["+", "i", 1]]]]]]}]}'::jsonb AS STATE
  UNION ALL
  SELECT
    CASE
      WHEN frame_type = 'expr'
      THEN CASE WHEN jsonb_typeof(expr) = 'number'
                THEN jsonb_build_object('stack', stack - 0, 'result', expr)
                WHEN jsonb_typeof(expr) = 'string'
                THEN jsonb_build_object('stack', stack - 0, 'result', env -> expr_string)
                WHEN op_string = 'if'
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env))  || (stack - 0))
                WHEN op_string = 'lambda'
                THEN jsonb_build_object('stack', stack - 0, 'result', jsonb_build_object('args', arg1, 'body', arg2, 'env', env))
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', expr, 'done', '[]'::jsonb, 'env', env))  || (stack - 0))
           END
      WHEN frame_type = 'eval_args'
      THEN CASE WHEN result IS NULL AND jsonb_array_length(args_left) = 0
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_call', 'expr', args_done, 'env', env)) || (stack - 0))
                WHEN result IS NULL
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', args_left -> 0, 'env', env), jsonb_build_object('type', 'eval_args', 'left', args_left - 0, 'done', args_done, 'env', env)) || stack - 0)
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', args_left, 'done', args_done || jsonb_build_array(result), 'env', env)) || (stack - 0))
           END
      WHEN frame_type = 'eval_call'
      THEN CASE WHEN op_string = '+'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint + arg2::text::bigint)
                WHEN op_string = '*'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint * arg2::text::bigint)
                WHEN op_string = '-'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint - arg2::text::bigint)
                WHEN op_string = '/'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint / arg2::text::bigint)
                WHEN op_string = '>'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint > arg2::text::bigint)
                WHEN op_string = '<'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint < arg2::text::bigint)
                WHEN op_string = '='
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 = arg2)
                WHEN op_string = 'head'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 -> 0)
                WHEN op_string = 'tail'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 - 0)
                WHEN op_string = 'cons'
                THEN jsonb_build_object('stack', stack - 0, 'result', jsonb_build_array(arg1) || arg2)
                WHEN op_string = 'empty'
                THEN jsonb_build_object('stack', stack - 0, 'result', '[]'::jsonb)
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr',
                     'expr', (op -> 'body'),
                     'env', (op -> 'env') || jsonb_build_object(
                       COALESCE(op -> 'args' ->> 0, 'null'), arg1,
                       COALESCE(op -> 'args' ->> 1, 'null'), arg2,
                       COALESCE(op -> 'args' ->> 2, 'null'), arg3,
                       COALESCE(op -> 'args' ->> 3, 'null'), arg4)))
                  || (stack - 0))
           END
      WHEN frame_type = 'eval_if'
      THEN CASE WHEN result IS NULL
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg1, 'env', env)) || stack)
                WHEN result IS NOT NULL AND result::text::boolean
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg2, 'env', env)) || (stack - 0))
                WHEN result IS NOT NULL AND NOT result::text::boolean
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg3, 'env', env)) || (stack - 0))
           END
      END
    FROM (
      SELECT state -> 'stack' -> 0 ->> 'type' AS frame_type,
             state -> 'stack' -> 0 -> 'expr' AS expr,
             state -> 'stack' -> 0 ->> 'expr' AS expr_string,
             state -> 'stack' -> 0 -> 'expr' -> 0 AS op,
             state -> 'stack' -> 0 -> 'expr' ->> 0 AS op_string,
             state -> 'stack' -> 0 -> 'expr' -> 1 AS arg1,
             state -> 'stack' -> 0 -> 'expr' -> 2 AS arg2,
             state -> 'stack' -> 0 -> 'expr' -> 3 AS arg3,
             state -> 'stack' -> 0 -> 'expr' -> 4 AS arg4,
             state -> 'stack' -> 0 -> 'left' AS args_left,
             state -> 'stack' -> 0 -> 'done' AS args_done,
             state -> 'stack' -> 0 -> 'env' AS env,
             state -> 'result' AS result,
             state -> 'stack' AS stack
             FROM loop
  ) sub
) SELECT state -> 'result' FROM loop WHERE jsonb_array_length(state -> 'stack') = 0 LIMIT 1;
