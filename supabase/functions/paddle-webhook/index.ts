import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const PADDLE_WEBHOOK_SECRET = Deno.env.get("PADDLE_WEBHOOK_SECRET") ?? "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const encoder = new TextEncoder();

function timingSafeEqual(a: string, b: string) {
  if (a.length !== b.length) return false;
  let result = 0;
  for (let i = 0; i < a.length; i += 1) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return result === 0;
}

async function verifySignature(signatureHeader: string | null, body: string) {
  if (!signatureHeader || !PADDLE_WEBHOOK_SECRET) return false;
  const parts = signatureHeader.split(";").map((part) => part.trim());
  const timestamp = parts.find((part) => part.startsWith("ts="))?.slice(3);
  const signature = parts.find((part) => part.startsWith("h1="))?.slice(3);
  if (!timestamp || !signature) return false;

  const payload = `${timestamp}:${body}`;
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(PADDLE_WEBHOOK_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const digest = await crypto.subtle.sign("HMAC", key, encoder.encode(payload));
  const hex = Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");

  return timingSafeEqual(hex, signature);
}

function parseCustomData(value: any) {
  if (!value) return null;
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return value;
}

function extractUserId(data: any) {
  const customData =
    parseCustomData(data?.custom_data)
    || parseCustomData(data?.customer?.custom_data)
    || parseCustomData(data?.subscription?.custom_data)
    || parseCustomData(data?.transaction?.custom_data)
    || parseCustomData(data?.checkout?.custom_data)
    || null;

  return customData?.user_id || customData?.userId || null;
}

function extractEmail(data: any) {
  const customData =
    parseCustomData(data?.custom_data)
    || parseCustomData(data?.customer?.custom_data)
    || parseCustomData(data?.subscription?.custom_data)
    || parseCustomData(data?.transaction?.custom_data)
    || parseCustomData(data?.checkout?.custom_data)
    || null;

  return (
    customData?.email
    || data?.customer?.email
    || data?.customer_email
    || data?.email
    || null
  );
}

function extractPriceId(data: any) {
  return (
    data?.items?.[0]?.price_id
    || data?.items?.[0]?.price?.id
    || data?.price_id
    || data?.price?.id
    || null
  );
}

serve(async (req) => {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const body = await req.text();
  const signatureHeader = req.headers.get("Paddle-Signature");
  const signatureOk = await verifySignature(signatureHeader, body);

  if (!signatureOk) {
    return new Response("Invalid signature", { status: 401 });
  }

  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    return new Response("Invalid payload", { status: 400 });
  }

  const eventType = payload.event_type || payload.eventType;
  const data = payload.data || {};

  const email = extractEmail(data);
  let userId = extractUserId(data);
  const subscriptionId =
    data.id || data.subscription_id || data.subscription?.id || null;
  const customerId =
    data.customer_id || data.customer?.id || data.customer?.customer_id || null;
  const priceId = extractPriceId(data);
  const rawStatus = data.status || null;
  const normalizedStatus = normalizeStatus(eventType, rawStatus);

  if (!userId && email) {
    const match = await findUserByEmail(email.toLowerCase());
    if (match?.id) {
      userId = match.id;
    } else {
      const { data: invited, error: inviteError } =
        await supabase.auth.admin.inviteUserByEmail(email);
      if (inviteError || !invited?.user?.id) {
        console.error("Invite user failed:", inviteError);
        return new Response("Invite failed", { status: 500 });
      }
      userId = invited.user.id;
    }
  }

  if (!userId) {
    console.warn("Missing user id and email", {
      eventType,
      dataKeys: Object.keys(data || {}),
    });
    return new Response("Missing user id", { status: 200 });
  }

  const { error } = await supabase.from("paddle_subscriptions").upsert(
    {
      user_id: userId,
      paddle_customer_id: customerId,
      paddle_subscription_id: subscriptionId,
      status: normalizedStatus,
      price_id: priceId,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "user_id" }
  );

  if (error) {
    console.error("Upsert error:", error);
    return new Response("Database error", { status: 500 });
  }

  return new Response("ok", { status: 200 });
});

function normalizeStatus(eventType: string | null, status: string | null) {
  const normalized = status ? String(status).toLowerCase() : null;
  if (!eventType) return normalized;
  if (eventType === "subscription.activated") return "active";
  if (eventType === "transaction.paid") return "active";
  if (eventType === "subscription.canceled") return "canceled";
  if (normalized === "complete" || normalized === "completed") return "active";
  if (normalized === "trialing") return "trialing";
  if (normalized === "active") return "active";
  return normalized;
}

async function findUserByEmail(email: string) {
  let page = 1;
  const perPage = 1000;
  // Iterate through pages until we find a match or exhaust results.
  while (true) {
    const { data, error } = await supabase.auth.admin.listUsers({
      page,
      perPage,
    });
    if (error || !data?.users) return null;
    const match = data.users.find(
      (user) => (user.email || "").toLowerCase() === email
    );
    if (match) return match;
    if (data.users.length < perPage) return null;
    page += 1;
  }
}
