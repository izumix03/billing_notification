use std::fmt::Write;
use aws_lambda_events::eventbridge::EventBridgeEvent;
use aws_sdk_costexplorer as costexplorer;
use aws_sdk_costexplorer::types::{DateInterval, Granularity, Group, GroupDefinition, GroupDefinitionType, Metric, MetricValue};
use chrono::{Datelike, Months};
use lambda_runtime::{service_fn, LambdaEvent};
use lambda_runtime::tower::ServiceExt;
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde_json::Value;

type MyError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    lambda_runtime::run(service_fn(lambda_handler)).await?;
    Ok(())
}

async fn lambda_handler(
    _event: LambdaEvent<EventBridgeEvent<serde_json::Value>>,
) -> Result<(), lambda_runtime::Error> {
    let exchange_rate = fetch_exchange_rate().await?;
    let cost_and_usages = fetch_cost_and_usage().await?;
    let current_month_cost_forecast = fetch_current_month_cost_forecast().await?;

    let total_cost: f64 = cost_and_usages.iter()
        .filter_map(|group| group.metrics.as_ref())
        .flat_map(|metrics| metrics.values().cloned().collect::<Vec<_>>())
        .filter_map(|metric| metric.amount)
        .filter_map(|amount| amount.parse::<f64>().ok())
        .sum();
    println!("total_cost: {}", total_cost);

    let monthly_cost = fetch_current_month_cost().await?;

    let formatted_total_cost = format_cost(total_cost, exchange_rate);
    println!("formatted_total_cost: {}", formatted_total_cost);

    let formatted_cost_per_service = format_service_costs(&cost_and_usages, exchange_rate, 5)?;
    println!("formatted_cost_per_service: {}", formatted_cost_per_service);

    let formatted_current_month_cost_forecast = format_cost(current_month_cost_forecast, exchange_rate);
    println!("formatted_forecast: {}", formatted_current_month_cost_forecast);

    let formatted_monthly_cost = format_cost(monthly_cost, exchange_rate);
    println!("formatted_monthly_cost: {}", formatted_monthly_cost);

    let content = format!("前々日料金:{formatted_total_cost}
--------------
現時点料金:{formatted_monthly_cost}
今月の予測:{formatted_current_month_cost_forecast}
■前々日の料金ランキング
{formatted_cost_per_service}
");
    println!("{}", content);

    Ok(())
}

fn format_cost(cost_usd: f64, exchange_rate: f64) -> String {
    let cost_jpy = cost_usd * exchange_rate;
    let rounded_jpy = cost_jpy.round();
    let rounded_usd = Decimal::from_f64(cost_usd).map(|d| d.round_dp(2)).unwrap_or_else(|| Decimal::ZERO);
    format!("{rounded_jpy}円(${rounded_usd})")
}

fn format_service_costs(cost_and_usages: &[Group], exchange_rate: f64, display_count: i8) -> Result<String, MyError> {
    let mut formatted_cost_per_service = String::new();

    for cost in cost_and_usages.iter().take(display_count as usize) {
        if let Some(keys) = &cost.keys {
            if let Some(key) = keys.first() {
                if let Some(metrics) = &cost.metrics {
                    if let Some(formatted_cost) = metrics.get("UnblendedCost").and_then(|metric| compute_formatted_cost(metric, exchange_rate)) {
                        writeln!(formatted_cost_per_service, "{:<50}:  {}", key, formatted_cost)?;
                    }
                }
            }
        }
    }
    Ok(format!("```\n{}\n```", formatted_cost_per_service))
}

fn compute_formatted_cost(metric: &MetricValue, exchange_rate: f64) -> Option<String> {
    metric.amount.as_ref()
        .and_then(|amount| amount.parse::<f64>().ok())
        .map(|amount| format_cost(amount, exchange_rate))
}

/// 1 USD あたりの JPY の逆レートを返す
/// Returns the inverse rate of JPY per USD
async fn fetch_exchange_rate() -> Result<f64, MyError> {
    let url = "https://www.floatrates.com/daily/jpy.json";
    let json: Value = Client::new().get(url).send().await?.json().await?;
    json["usd"]["inverseRate"].as_f64().ok_or_else(|| "USDレートをf64に変換できませんでした".into())
}

/// 2日前から昨日までの利用料金を返す
async fn fetch_cost_and_usage() -> Result<Vec<Group>, MyError> {
    let day_before_yesterday = chrono::Utc::now().date_naive() - chrono::Duration::days(2);
    let yesterday = chrono::Utc::now().date_naive() - chrono::Duration::days(1);

    let config = aws_config::load_from_env().await;
    let client = costexplorer::Client::new(&config);
    let result = client.get_cost_and_usage()
        .time_period(DateInterval::builder().start(day_before_yesterday.to_string()).end(yesterday.to_string()).build()?)
        .granularity(Granularity::Daily)
        .metrics("UnblendedCost")
        .group_by(GroupDefinition::builder().r#type(GroupDefinitionType::Dimension).key("SERVICE").build())
        .send()
        .await?;
    let mut groups = result.results_by_time.and_then(|mut rbt| rbt.pop()).and_then(|first| first.groups).ok_or_else(|| "No groups found in the first result".to_string())?;
    groups.sort_by(|a, b| {
        let a_cost = get_unblended_cost(a);
        let b_cost = get_unblended_cost(b);
        b_cost.partial_cmp(&a_cost).unwrap()
    });
    println!("{:?}", groups);
    Ok(groups)
}

fn get_unblended_cost(group: &Group) -> f64 {
    group.metrics.as_ref().and_then(|metrics| metrics.get("UnblendedCost")).and_then(|cost| cost.amount.as_ref()).and_then(|amount| amount.parse::<f64>().ok()).unwrap_or(0.0)
}

async fn fetch_current_month_cost_forecast() -> Result<f64, MyError> {
    let today = chrono::Utc::now().date_naive();
    let next_month_1st = chrono::Utc::now().date_naive().checked_add_months(Months::new(1)).and_then(|d| d.with_day(1)).ok_or_else(|| "Failed to calculate the first day of next month".to_string())?;
    let config = aws_config::load_from_env().await;
    let client = costexplorer::Client::new(&config);
    let result = client.get_cost_forecast().time_period(DateInterval::builder().start(today.to_string()).end(next_month_1st.to_string()).build()?).metric(Metric::UnblendedCost).granularity(Granularity::Monthly).send().await?;
    Ok(result.total.and_then(|total| total.amount).and_then(|amount| amount.parse::<f64>().ok()).ok_or_else(|| "Failed to parse the forecasted cost".to_string())?)
}

async fn fetch_current_month_cost() -> Result<f64, MyError> {
    let current_month_1th = chrono::Utc::now().date_naive().with_day(1).ok_or_else(|| "Failed to calculate the first day of this month".to_string())?;
    let next_month_1st = chrono::Utc::now().date_naive()
        .checked_add_months(Months::new(1))
        .and_then(|d| d.with_day(1))
        .ok_or_else(|| "Failed to calculate the first day of next month".to_string())?;

    let config = aws_config::load_from_env().await;
    let client = costexplorer::Client::new(&config);
    let result = client.get_cost_and_usage()
        .time_period(
            DateInterval::builder()
                .start(current_month_1th.to_string())
                .end(next_month_1st.to_string())
                .build()?
        )
        .granularity(Granularity::Monthly)
        .metrics("UnblendedCost")
        .send()
        .await?;

    let total_cost = result.results_by_time
        .and_then(|results_by_time| results_by_time.first().cloned())
        .and_then(|result_by_time| result_by_time.total)
        .and_then(|total| total.get("UnblendedCost").cloned())
        .and_then(|cost| cost.amount)
        .and_then(|amount| amount.parse::<f64>().ok())
        .ok_or_else(|| "Failed to extract this month cost amount")?;

    Ok(total_cost)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lambda_handler() {
        let event = LambdaEvent::new(EventBridgeEvent::default(), Default::default());
        let result = lambda_handler(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_exchange_rate() {
        let result = fetch_exchange_rate().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_cost_and_usage() {
        let result = fetch_cost_and_usage().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_current_month_cost_forecast() {
        let result = fetch_current_month_cost_forecast().await;
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_current_month_cost() {
        let result = fetch_current_month_cost().await;
        println!("{:?}", result);
        assert!(result.is_ok());
    }
}
