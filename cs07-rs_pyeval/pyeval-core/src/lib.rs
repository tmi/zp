use pest_derive::Parser;
use pest::iterators::Pair;
use pest::Parser;
use std::collections::HashMap;
use std::collections::HashSet;
use chrono::{DateTime, NaiveDateTime, Duration, Timelike, Utc, TimeZone};
use regex::Regex;
use lazy_static::lazy_static;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct EvalParser;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    DateTime(DateTime<Utc>),
    TimeDelta(Duration),
    List(Vec<Value>),
    #[allow(dead_code)]
    Map(HashMap<String, Value>),
    Function(String),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => {
                if fl.fract() == 0.0 && !fl.is_infinite() && !fl.is_nan() {
                    write!(f, "{:.1}", fl)
                } else {
                    write!(f, "{}", fl)
                }
            }
            Value::DateTime(dt) => write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S")),
            Value::TimeDelta(d) => write!(f, "{:?}", d),
            Value::List(l) => write!(f, "{:?}", l),
            Value::Map(m) => write!(f, "{:?}", m),
            Value::Function(n) => write!(f, "<function {}>", n),
        }
    }
}

lazy_static! {
    static ref DATETIME_RE: Regex = Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$").unwrap();
    static ref INTERPOLATION_RE: Regex = Regex::new(r"\$\{([^}]*)\}").unwrap();
    static ref BUILTIN_NAMES: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("timedelta");
        set.insert("floor_day");
        set.insert("floor_hour");
        set.insert("upper");
        set.insert("lower");
        set.insert("split");
        set
    };
}

pub fn coerce_value(s: &str) -> Value {
    if DATETIME_RE.is_match(s) {
        let normalized = s.replace(" ", "T");
        if let Ok(dt) = NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%dT%H:%M:%S") {
            return Value::DateTime(Utc.from_utc_datetime(&dt));
        }
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    Value::String(s.to_string())
}

pub struct Evaluator<'a> {
    variables: &'a HashMap<String, Value>,
}

impl<'a> Evaluator<'a> {
    pub fn new(variables: &'a HashMap<String, Value>) -> Self {
        Self { variables }
    }

    pub fn eval(&self, pair: Pair<Rule>) -> Result<Value, String> {
        match pair.as_rule() {
            Rule::expression => self.eval(pair.into_inner().next().unwrap()),
            Rule::binary_expr | Rule::term | Rule::factor => {
                let mut inner = pair.into_inner();
                let mut result = self.eval(inner.next().unwrap())?;
                while let Some(op_pair) = inner.next() {
                    let op = op_pair.as_str();
                    let next_val = self.eval(inner.next().unwrap())?;
                    result = self.apply_binary_op(result, op, next_val)?;
                }
                Ok(result)
            }
            Rule::unary | Rule::add_op | Rule::mul_op | Rule::pow_op => self.eval(pair.into_inner().next().unwrap()),
            Rule::postfix_expr => {
                let mut inner = pair.into_inner();
                let mut result = self.eval(inner.next().unwrap())?;
                for postfix in inner {
                    result = self.apply_postfix(result, postfix)?;
                }
                Ok(result)
            }
            Rule::primary_expr => self.eval(pair.into_inner().next().unwrap()),
            Rule::paren => self.eval(pair.into_inner().next().unwrap()),
            Rule::number => {
                let s = pair.as_str();
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    s.parse::<f64>().map(Value::Float).map_err(|e| e.to_string())
                } else {
                    s.parse::<i64>().map(Value::Int).map_err(|e| e.to_string())
                }
            }
            Rule::string => {
                let s = pair.as_str();
                let content = &s[1..s.len() - 1];
                Ok(Value::String(content.replace("\\\"", "\"").replace("\\'", "'")))
            }
            Rule::var_ref => {
                let name = pair.as_str();
                if let Some(val) = self.variables.get(name) {
                    Ok(val.clone())
                } else {
                    match name {
                        "timedelta" | "floor_day" | "floor_hour" | "upper" | "lower" => Ok(Value::Function(name.to_string())),
                        _ => Err(format!("Variable not found: {}", name)),
                    }
                }
            }
            Rule::func_call => {
                let mut inner = pair.into_inner();
                let name = inner.next().unwrap().as_str();
                let args = if let Some(arglist) = inner.next() {
                    self.eval_arglist(arglist)?
                } else {
                    (vec![], HashMap::new())
                };
                self.call_builtin(name, args)
            }
            _ => Err(format!("Unexpected rule: {:?}", pair.as_rule())),
        }
    }

    fn eval_arglist(&self, pair: Pair<Rule>) -> Result<(Vec<Value>, HashMap<String, Value>), String> {
        let mut pos_args = vec![];
        let mut kw_args = HashMap::new();
        for arg in pair.into_inner() {
            let inner = arg.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::posarg => {
                    pos_args.push(self.eval(inner.into_inner().next().unwrap())?);
                }
                Rule::kwarg => {
                    let mut kw_inner = inner.into_inner();
                    let name = kw_inner.next().unwrap().as_str().to_string();
                    let val = self.eval(kw_inner.next().unwrap())?;
                    kw_args.insert(name, val);
                }
                _ => unreachable!(),
            }
        }
        Ok((pos_args, kw_args))
    }

    fn call_builtin(&self, name: &str, args: (Vec<Value>, HashMap<String, Value>)) -> Result<Value, String> {
        let (pos, kw) = args;
        match name {
            "timedelta" => {
                let mut days = 0;
                if let Some(Value::Int(d)) = kw.get("days") {
                    days = *d;
                } else if let Some(Value::Int(d)) = pos.get(0) {
                    days = *d;
                }
                Ok(Value::TimeDelta(Duration::days(days)))
            }
            "floor_day" => {
                if let Some(Value::DateTime(dt)) = pos.get(0) {
                    let d = dt.date_naive();
                    let t = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                    Ok(Value::DateTime(Utc.from_utc_datetime(&NaiveDateTime::new(d, t))))
                } else {
                    Err("floor_day expects a datetime".into())
                }
            }
            "floor_hour" => {
                if let Some(Value::DateTime(dt)) = pos.get(0) {
                    let d = dt.date_naive();
                    let t = chrono::NaiveTime::from_hms_opt(dt.hour(), 0, 0).unwrap();
                    Ok(Value::DateTime(Utc.from_utc_datetime(&NaiveDateTime::new(d, t))))
                } else {
                    Err("floor_hour expects a datetime".into())
                }
            }
            "upper" => {
                if let Some(Value::String(s)) = pos.get(0) {
                    Ok(Value::String(s.to_uppercase()))
                } else {
                    Err("upper expects a string".into())
                }
            }
            "lower" => {
                if let Some(Value::String(s)) = pos.get(0) {
                    Ok(Value::String(s.to_lowercase()))
                } else {
                    Err("lower expects a string".into())
                }
            }
            _ => Err(format!("Unknown function: {}", name)),
        }
    }

    fn apply_binary_op(&self, left: Value, op: &str, right: Value) -> Result<Value, String> {
        match (left, op, right) {
            (Value::Int(a), "+", Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), "+", Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), "+", Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
            (Value::Float(a), "+", Value::Int(b)) => Ok(Value::Float(a + b as f64)),
            (Value::String(a), "+", Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),

            (Value::Int(a), "-", Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), "-", Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), "-", Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
            (Value::Float(a), "-", Value::Int(b)) => Ok(Value::Float(a - b as f64)),

            (Value::Int(a), "*", Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), "*", Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), "*", Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
            (Value::Float(a), "*", Value::Int(b)) => Ok(Value::Float(a * b as f64)),

            (Value::Int(a), "/", Value::Int(b)) => Ok(Value::Float(a as f64 / b as f64)),
            (Value::Float(a), "/", Value::Float(b)) => Ok(Value::Float(a / b)),
            (Value::Int(a), "/", Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
            (Value::Float(a), "/", Value::Int(b)) => Ok(Value::Float(a / b as f64)),

            (Value::Int(a), "**", Value::Int(b)) => {
                if b >= 0 {
                    Ok(Value::Int(a.pow(b as u32)))
                } else {
                    Ok(Value::Float((a as f64).powf(b as f64)))
                }
            }
            (Value::Float(a), "**", Value::Float(b)) => Ok(Value::Float(a.powf(b))),
            (Value::Int(a), "**", Value::Float(b)) => Ok(Value::Float((a as f64).powf(b))),
            (Value::Float(a), "**", Value::Int(b)) => Ok(Value::Float(a.powf(b as f64))),

            (Value::Int(a), "//", Value::Int(b)) => Ok(Value::Int(a / b)),

            (Value::Int(a), "%", Value::Int(b)) => Ok(Value::Int(a % b)),

            (Value::DateTime(dt), "+", Value::TimeDelta(d)) => Ok(Value::DateTime(dt + d)),
            (Value::DateTime(dt), "-", Value::TimeDelta(d)) => Ok(Value::DateTime(dt - d)),

            (a, op, b) => Err(format!("Unsupported binary op: {:?} {} {:?}", a, op, b)),
        }
    }

    fn apply_postfix(&self, obj: Value, postfix: Pair<Rule>) -> Result<Value, String> {
        match postfix.as_rule() {
            Rule::subscript => {
                let index_expr = postfix.into_inner().next().unwrap();
                let index = self.eval(index_expr)?;
                match (&obj, index) {
                    (Value::List(l), Value::Int(i)) => {
                        let idx = if i < 0 { (l.len() as i64 + i) as usize } else { i as usize };
                        Ok(l.get(idx).cloned().ok_or("Index out of bounds")?)
                    }
                    _ => Err("Subscript only supported for lists with int index for now".into()),
                }
            }
            Rule::method_call => {
                let mut inner = postfix.into_inner();
                let method_name = inner.next().unwrap().as_str();
                let args = if let Some(arglist) = inner.next() {
                    self.eval_arglist(arglist)?
                } else {
                    (vec![], HashMap::new())
                };
                match (obj.clone(), method_name) {
                    (Value::String(s), "split") => {
                        let sep = if let Some(Value::String(sep)) = args.0.get(0) {
                            sep.as_str()
                        } else {
                            " "
                        };
                        let limit = if let Some(Value::Int(l)) = args.0.get(1) {
                            Some(*l as usize)
                        } else {
                            None
                        };

                        let parts: Vec<Value> = if let Some(l) = limit {
                            s.splitn(l + 1, sep).map(|p| Value::String(p.to_string())).collect()
                        } else {
                            s.split(sep).map(|p| Value::String(p.to_string())).collect()
                        };
                        Ok(Value::List(parts))
                    }
                    (Value::String(s), "lower") => Ok(Value::String(s.to_lowercase())),
                    (Value::String(s), "upper") => Ok(Value::String(s.to_uppercase())),
                    _ => Err(format!("Unsupported method call: {} on {:?}", method_name, obj)),
                }
            }
            _ => Err(format!("Unexpected postfix rule: {:?}", postfix.as_rule())),
        }
    }
}

pub fn resolve_expression(raw: &str, variables: &HashMap<String, String>) -> Result<String, String> {
    let coerced_vars: HashMap<String, Value> = variables.iter().map(|(k, v)| (k.clone(), coerce_value(v))).collect();
    let evaluator = Evaluator::new(&coerced_vars);

    let mut result = raw.to_string();
    let mut offset = 0;

    for mat in INTERPOLATION_RE.find_iter(raw) {
        let full_match = mat.as_str();
        let expr_str = &full_match[2..full_match.len() - 1].trim();

        let pairs = EvalParser::parse(Rule::expression, expr_str).map_err(|e| format!("Parse error in '{}': {}", expr_str, e))?;
        let val = evaluator.eval(pairs.into_iter().next().unwrap())?;
        let val_str = val.to_string();

        let start = mat.start() as i64 + offset;
        let end = mat.end() as i64 + offset;
        result.replace_range((start as usize)..(end as usize), &val_str);
        offset += val_str.len() as i64 - (mat.end() - mat.start()) as i64;
    }

    Ok(result)
}

pub fn extract_glyphs(raw: &str) -> Result<HashSet<String>, String> {
    let mut glyphs = HashSet::new();
    for mat in INTERPOLATION_RE.find_iter(raw) {
        let full_match = mat.as_str();
        let expr_str = &full_match[2..full_match.len() - 1].trim();
        let pairs = EvalParser::parse(Rule::expression, expr_str).map_err(|e| format!("Parse error in '{}': {}", expr_str, e))?;
        collect_glyphs(pairs.into_iter().next().unwrap(), &mut glyphs);
    }
    Ok(glyphs)
}

fn collect_glyphs(pair: Pair<Rule>, glyphs: &mut HashSet<String>) {
    match pair.as_rule() {
        Rule::var_ref => {
            let name = pair.as_str();
            if !BUILTIN_NAMES.contains(name) {
                glyphs.insert(name.to_string());
            }
        }
        _ => {
            for inner in pair.into_inner() {
                collect_glyphs(inner, glyphs);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_simple() {
        let mut vars = HashMap::new();
        vars.insert("var1".to_string(), "hello".to_string());
        vars.insert("var2".to_string(), "world".to_string());
        assert_eq!(resolve_expression("${var1} literal ${var2}", &vars).unwrap(), "hello literal world");
    }

    #[test]
    fn test_resolve_datetime() {
        let mut vars = HashMap::new();
        vars.insert("submitDatetime".to_string(), "2024-03-15 06:00:00".to_string());
        assert_eq!(resolve_expression("${submitDatetime + timedelta(days=1)}", &vars).unwrap(), "2024-03-16 06:00:00");
    }

    #[test]
    fn test_resolve_floor_day() {
        let mut vars = HashMap::new();
        vars.insert("submitDatetime".to_string(), "2024-03-15 06:00:00".to_string());
        assert_eq!(resolve_expression("${floor_day(submitDatetime)}", &vars).unwrap(), "2024-03-15 00:00:00");
    }

    #[test]
    fn test_resolve_upper_lower() {
        let mut vars = HashMap::new();
        vars.insert("myParam1".to_string(), "hello".to_string());
        assert_eq!(resolve_expression("${upper(myParam1)}", &vars).unwrap(), "HELLO");

        let mut vars2 = HashMap::new();
        vars2.insert("myParam1".to_string(), "WORLD".to_string());
        assert_eq!(resolve_expression("${lower(myParam1)}", &vars2).unwrap(), "world");
    }

    #[test]
    fn test_resolve_split() {
        let mut vars = HashMap::new();
        vars.insert("myParam1".to_string(), "ERA5_daily".to_string());
        assert_eq!(resolve_expression("${myParam1.split('_', 1)[0]}", &vars).unwrap(), "ERA5");
    }

    #[test]
    fn test_resolve_math() {
        let vars = HashMap::new();
        assert_eq!(resolve_expression("${42 ** 10}", &vars).unwrap(), "17080198121677824");
    }

    #[test]
    fn test_resolve_math_coercion() {
        let mut vars = HashMap::new();
        vars.insert("a".to_string(), "2".to_string());
        vars.insert("b".to_string(), "3".to_string());
        vars.insert("c".to_string(), "4".to_string());
        assert_eq!(resolve_expression("${(a + b) * c}", &vars).unwrap(), "20");
    }

    #[test]
    fn test_resolve_precedence() {
        let vars = HashMap::new();
        assert_eq!(resolve_expression("${1 + 2 * 3}", &vars).unwrap(), "7");
        assert_eq!(resolve_expression("${(1 + 2) * 3}", &vars).unwrap(), "9");
    }

    #[test]
    fn test_resolve_scientific() {
        let vars = HashMap::new();
        assert_eq!(resolve_expression("${42 * 1e3 + 7}", &vars).unwrap(), "42007.0");
    }

    #[test]
    fn test_resolve_chained_methods() {
        let mut vars = HashMap::new();
        vars.insert("myString".to_string(), "Hello_World".to_string());
        assert_eq!(resolve_expression("${myString.split('_')[0].lower()}", &vars).unwrap(), "hello");
    }

    #[test]
    fn test_extract_glyphs() {
        assert_eq!(extract_glyphs("${submitDatetime + timedelta(days=1)} and ${upper(myParam1)}").unwrap(),
            vec!["submitDatetime".to_string(), "myParam1".to_string()].into_iter().collect::<HashSet<_>>());
    }
}
