use crate::error::AppError;
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use std::str::FromStr;
use rust_decimal::Decimal;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method, RequestBuilder};
use std::time::Duration;
use serde::{Deserialize, Serialize};

// CONSTANTS
#[allow(dead_code)]
const BASE_URL: &str = "https://api.bitget.com";

// --- NEW STRUCTS FOR SPOT ORDERS ---

/// Structure for the spot order request body.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: String, // "buy" or "sell"
    pub order_type: String, // "market" for our use case
    pub force: String,
    pub size: String, // Quantity in quote currency for market buy, in base currency for market sell

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Structure for the futures order request body.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlaceFuturesOrderRequest {
    pub symbol: String,
    pub product_type: String, // "USDT-FUTURES"
    pub margin_mode: String,  // "isolated"
    pub margin_coin: String,  // "USDT"
    pub size: String,         // Quantity in base asset, as a string
    pub side: String,         // "buy" or "sell"
    // "open" or "close" - not needed for one-way mode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    pub order_type: String,   // "market"

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Structure for the data field in a successful order placement response.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderResponse {
    pub order_id: String,
    pub client_oid: Option<String>,
}

/// Structure for the spot order info response.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpotOrderInfo {
    pub order_id: String,
    pub status: String,
    pub price_avg: String,
    pub base_volume: String, // Исполненный объем в базовой валюте
    pub quote_volume: String, // Исполненный объем в котируемой валюте
    pub client_oid: Option<String>,
}

/// Structure for the futures order info response.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FuturesOrderInfo {
    pub order_id: String,
    #[serde(rename = "state")] // Используем serde(rename), так как поле называется 'state'
    pub status: String,
    pub price_avg: String,
    pub base_volume: String, // Исполненный объем
    pub client_oid: Option<String>,
}

/// --- НОВЫЕ СТРУКТУРЫ ДЛЯ БАЛАНСА ---
#[derive(Deserialize, Debug)]
#[derive(Clone)] // Добавляем Clone для возможности клонирования в логировании
#[serde(rename_all = "camelCase")]
pub struct SpotAccountAsset {
    pub coin: String,
    pub available: String, // Нам нужен именно доступный для торговли баланс
    // ... (frozen, lock - нам не нужны)
}

/// Structure for setting margin mode.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SetMarginModeRequest {
    pub symbol: String,
    pub product_type: String, // "USDT-FUTURES"
    pub margin_mode: String,  // "isolated" or "cross"
    pub margin_coin: String,  // "USDT"
}

/// Structure for setting leverage.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SetLeverageRequest {
    pub symbol: String,
    pub product_type: String, // "USDT-FUTURES"
    pub margin_coin: String,  // "USDT"
    pub leverage: String,
    pub hold_side: Option<String>, // "long" or "short"
}

/// Structure for setting position mode.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SetPositionModeRequest {
    pub product_type: String, // "USDT-FUTURES"
    pub pos_mode: String,     // "one_way_mode"
}

/// Generic API response structure from Bitget.
#[derive(Deserialize, Debug)]
pub struct ApiResponse<T> {
    pub code: String,
    pub msg: String,
    pub data: Option<T>,
}

/// Specialized API response for the spot assets endpoint, which uses "message" instead of "msg".
#[derive(Deserialize, Debug)]
pub struct ApiAssetResponse<T> {
    pub code: String,
    pub message: String, // <-- ПРАВИЛЬНОЕ ИМЯ ПОЛЯ
    pub data: Option<T>,
}

#[allow(dead_code)]
pub struct ApiClient {
    client: Client,
    api_key: String,
    secret_key: String,
    passphrase: String,
}

impl ApiClient {
    pub fn new(api_key: String, secret_key: String, passphrase: String) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(10)) // Общий таймаут на весь запрос
                .build()
                .expect("Failed to build reqwest client"),
            api_key,
            secret_key,
            passphrase,
        }
    }

    /// Creates a signed reqwest::RequestBuilder for a given API endpoint.
    /// This private helper method encapsulates the authentication logic.
    fn create_signed_request<T: Serialize>(
        &self,
        method: Method,
        request_path: &str,
        query_string: Option<&str>, // Добавляем опциональный параметр
        body: T,
    ) -> Result<RequestBuilder, AppError> {
        // ИСПРАВЛЕНИЕ 1: Используем Unix timestamp в миллисекундах
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_str = serde_json::to_string(&body)?;

        // --- ИСПРАВЛЕНИЕ: Формируем строку по правилам ---
        let message_to_sign = if let Some(qs) = query_string {
            format!("{}{}{}?{}{}", &timestamp, method.as_str(), request_path, qs, &body_str)
        } else {
            format!("{}{}{}{}", &timestamp, method.as_str(), request_path, &body_str)
        };

        type HmacSha256 = Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message_to_sign.as_bytes());
        let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        
        let url = if let Some(qs) = query_string {
            format!("{}{}?{}", BASE_URL, request_path, qs)
        } else {
            format!("{}{}", BASE_URL, request_path)
        };

        let request_builder = self.client.request(method, &url)
            // ИСПРАВЛЕНИЕ: Используем правильные имена заголовков V2 без префикса "B-"
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-SIGN", &signature)
            .header("ACCESS-TIMESTAMP", &timestamp)
            .header("ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json")
            .body(body_str);

        Ok(request_builder)
    }

    /// Creates and signs a GET request (without a body).
    fn create_signed_get_request(
        &self,
        request_path: &str,
        query_params: &str, // URL parameters, e.g., "symbol=BTCUSDT&orderId=123"
    ) -> Result<RequestBuilder, AppError> {
        let timestamp = Utc::now().timestamp_millis().to_string();

        // --- ИСПРАВЛЕНИЕ: Добавляем "?" ---
        // String to sign for GET: timestamp + method + path + "?" + query_params
        // Знак '?' нужен всегда, даже если query_params пустой.
        let message_to_sign = if query_params.is_empty() {
            format!("{}{}{}", &timestamp, "GET", request_path)
        } else {
            format!("{}{}{}?{}", &timestamp, "GET", request_path, query_params)
        };

        type HmacSha256 = Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message_to_sign.as_bytes());
        let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let url = if query_params.is_empty() {
            format!("{}{}", BASE_URL, request_path)
        } else {
            format!("{}{}?{}", BASE_URL, request_path, query_params)
        };

        let request_builder = self.client.get(&url)
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-SIGN", &signature)
            .header("ACCESS-TIMESTAMP", &timestamp)
            .header("ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json");

        Ok(request_builder)
    }

    /// Places a market order on the spot market.
    pub async fn place_spot_order(&self, order: PlaceOrderRequest) -> Result<PlaceOrderResponse, AppError> {
        let path = "/api/v2/spot/trade/place-order";

        if order.order_type != "market" {
            return Err(AppError::LogicError("Only market orders are supported".to_string()));
        }

        let builder = self.create_signed_request(Method::POST, path, None, order)?;
        let response = builder.send().await?;
        let api_response: ApiResponse<PlaceOrderResponse> = response.json().await?;

        api_response.data.ok_or_else(|| AppError::ApiError(api_response.code, api_response.msg))
    }

    /// Places a market order on the futures market.
    pub async fn place_futures_order(
        &self,
        order: PlaceFuturesOrderRequest,
    ) -> Result<PlaceOrderResponse, AppError> {
        let path = "/api/v2/mix/order/place-order";

        if order.order_type != "market" {
            return Err(AppError::LogicError("Only market orders are supported".to_string()));
        }

        let builder = self.create_signed_request(Method::POST, path, None, order)?;
        let response = builder.send().await?;
        let api_response: ApiResponse<PlaceOrderResponse> = response.json().await?;

        api_response.data.ok_or_else(|| AppError::ApiError(api_response.code, api_response.msg))
    }

    /// Gets information about a specific spot order.
    pub async fn get_spot_order(
        &self,
        order_id: &str,
    ) -> Result<OrderInfo, AppError> {
        let path = "/api/v2/spot/trade/orderInfo";
        let params = format!("orderId={}", order_id);

        let builder = self.create_signed_get_request(path, &params)?;
        let response = builder.send().await?;

        let api_response: ApiResponse<Vec<SpotOrderInfo>> = response.json().await?;

        if api_response.code != "00000" {
            return Err(AppError::ApiError(api_response.code, api_response.msg));
        }

        // The response comes as an array, we take the first element
        api_response.data
            .and_then(|mut v| v.pop().map(|spot_info| OrderInfo {
                status: spot_info.status,
                price_avg: spot_info.price_avg,
                base_volume: spot_info.base_volume,
                quote_volume: Some(spot_info.quote_volume),
                client_oid: spot_info.client_oid,
            }))
            .ok_or_else(|| AppError::ApiError(api_response.code, "API returned success but no order data".to_string()))
    }

    /// Gets information about a specific futures order.
    pub async fn get_futures_order(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<OrderInfo, AppError> {
        let path = "/api/v2/mix/order/detail";
        let params = format!("symbol={}&orderId={}&productType=USDT-FUTURES", symbol, order_id);

        let builder = self.create_signed_get_request(path, &params)?;
        let response = builder.send().await?;

        let api_response: ApiResponse<FuturesOrderInfo> = response.json().await?;

        // The response comes as a single object
        api_response.data.map(|fut_info| OrderInfo {
            status: fut_info.status,
            price_avg: fut_info.price_avg,
            base_volume: fut_info.base_volume,
            quote_volume: None,
            client_oid: fut_info.client_oid,
        }).ok_or_else(|| AppError::ApiError(api_response.code, api_response.msg))
    }

    /// --- НОВАЯ ФУНКЦИЯ В impl ApiClient ---
    /// Gets spot account balance for a specific coin.
    pub async fn get_spot_balance(&self, coin: &str) -> Result<Decimal, AppError> {
        let path = "/api/v2/spot/account/assets";
        // Передаем coin в верхнем регистре, как требует большинство API
        let params = format!("coin={}", coin.to_uppercase());

        let builder = self.create_signed_get_request(path, &params)?;
        let response = builder.send().await?;

        // Используем новую, правильную структуру для парсинга
        let api_response: ApiAssetResponse<Vec<SpotAccountAsset>> = response.json().await?;

        if api_response.code != "00000" {
            return Err(AppError::ApiError(api_response.code, api_response.message));
        }

        // Заимствуем `data` через `as_ref`, чтобы не перемещать его из `api_response`.
        let balance_opt = api_response.data.as_ref()
            .and_then(|assets| assets.first()) // Безопасно берем ссылку на первый элемент
            .and_then(|asset| Decimal::from_str(&asset.available).ok()); // Парсим `available` в Decimal

        if let Some(balance) = balance_opt {
            Ok(balance)
        } else {
            // Теперь `api_response` полностью доступна для логирования.
            tracing::warn!("[ApiClient] get_spot_balance for '{}' returned success code but data array was empty or balance could not be parsed. API Response: {:?}", coin, &api_response);
            Err(AppError::LogicError(format!("Could not find or parse available balance for {}", coin)))
        }
    }


    /// Sets the margin mode for a futures symbol.
    pub async fn set_margin_mode(&self, req: SetMarginModeRequest) -> Result<(), AppError> {
        let path = "/api/v2/mix/account/set-margin-mode";
        let builder = self.create_signed_request(Method::POST, path, None, req)?;
        let response = builder.send().await?;
        let api_response: ApiResponse<serde_json::Value> = response.json().await?;

        if api_response.code == "00000" {
            Ok(())
        } else {
            Err(AppError::ApiError(api_response.code, api_response.msg))
        }
    }

    /// Sets the leverage for a futures symbol.
    pub async fn set_leverage(&self, req: SetLeverageRequest) -> Result<(), AppError> {
        let path = "/api/v2/mix/account/set-leverage";
        let builder = self.create_signed_request(Method::POST, path, None, req)?;
        let response = builder.send().await?;
        let api_response: ApiResponse<serde_json::Value> = response.json().await?;

        if api_response.code == "00000" {
            Ok(())
        } else {
            // Bitget returns a specific error code if leverage is already set to the desired value.
            // We can treat this as a success.
            if api_response.code == "40513" { // "Leverage is not modified"
                Ok(())
            } else {
                Err(AppError::ApiError(api_response.code, api_response.msg))
            }
        }
    }

    /// Sets the position mode for futures.
    pub async fn set_position_mode(&self, req: SetPositionModeRequest) -> Result<(), AppError> {
        let path = "/api/v2/mix/account/set-position-mode";
        let builder = self.create_signed_request(Method::POST, path, None, req)?;
        let response = builder.send().await?;
        let api_response: ApiResponse<serde_json::Value> = response.json().await?;

        if api_response.code == "00000" {
            Ok(())
        } else {
            Err(AppError::ApiError(api_response.code, api_response.msg))
        }
    }
}

/// Общая структура для информации об ордере, чтобы унифицировать обработку в `order_watcher`.
#[derive(Debug, Clone)]
pub struct OrderInfo {
    pub status: String,
    pub price_avg: String,
    pub base_volume: String,
    pub quote_volume: Option<String>,
    pub client_oid: Option<String>,
}