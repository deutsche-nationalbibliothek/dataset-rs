use std::collections::HashMap;

use bstr::{BString, ByteSlice};
use ndarray::Array1;
use ndarray_stats::DeviationExt;
use unicode_normalization::UnicodeNormalization;

#[inline]
fn frequencies(buf: &BString, alphabet: &[char]) -> HashMap<char, u64> {
    buf.chars()
        .nfc()
        .to_string()
        .to_lowercase()
        .chars()
        .filter(|c| alphabet.contains(c))
        .fold(HashMap::new(), |mut freqs, value| {
            freqs
                .entry(value)
                .and_modify(|entry| *entry += 1)
                .or_insert(1);
            freqs
        })
}

pub(crate) fn lfreq_ger(buf: &BString) -> Option<f64> {
    let alphabet: Vec<char> =
        "abcdefghijklmnopqrstuvwxyzßäöü".chars().collect();

    let freqs = frequencies(buf, &alphabet);
    let n: f64 = freqs.values().sum::<u64>() as f64;
    let x = if n > 0.0 {
        Array1::from_iter(
            alphabet
                .iter()
                .map(|c| *freqs.get(c).unwrap_or(&0) as f64 / n),
        )
    } else {
        Array1::zeros(alphabet.len())
    };

    let y = Array1::from_vec(vec![
        0.06006, 0.02148, 0.02690, 0.04718, 0.16006, 0.01832, 0.03064,
        0.04249, 0.07752, 0.00297, 0.01536, 0.03787, 0.02798, 0.09660,
        0.02684, 0.01049, 0.00028, 0.07737, 0.06343, 0.06369, 0.03820,
        0.00918, 0.01427, 0.00051, 0.00107, 0.01237, 0.00170, 0.00548,
        0.00269, 0.00683,
    ]);

    x.l2_dist(&y).ok()
}

pub(crate) fn lfreq_eng(buf: &BString) -> Option<f64> {
    let alphabet: Vec<char> =
        "abcdefghijklmnopqrstuvwxyz".chars().collect();

    let freqs = frequencies(buf, &alphabet);
    let n: f64 = freqs.values().sum::<u64>() as f64;
    let x = if n > 0.0 {
        Array1::from_iter(
            alphabet
                .iter()
                .map(|c| *freqs.get(c).unwrap_or(&0) as f64 / n),
        )
    } else {
        Array1::zeros(alphabet.len())
    };

    let y = Array1::from_vec(vec![
        0.08167, 0.01492, 0.02782, 0.04253, 0.12702, 0.02228, 0.02015,
        0.06094, 0.06966, 0.00253, 0.01772, 0.04025, 0.02406, 0.06749,
        0.07507, 0.01929, 0.00950, 0.05987, 0.06327, 0.09056, 0.02758,
        0.00978, 0.02360, 0.00250, 0.01974, 0.00074,
    ]);

    x.l2_dist(&y).ok()
}

#[cfg(test)]
mod tests {
    use bstr::BString;

    type TestResult = anyhow::Result<()>;

    #[test]
    fn frequencies() -> TestResult {
        use super::frequencies;

        let alphabet: Vec<char> = "abcdef".chars().collect();
        let freqs = frequencies(&BString::from("abca"), &alphabet);

        assert_eq!(freqs.get(&'a').unwrap(), &2);
        assert_eq!(freqs.get(&'b').unwrap(), &1);
        assert_eq!(freqs.get(&'c').unwrap(), &1);
        assert_eq!(freqs.len(), 3);

        Ok(())
    }
}
