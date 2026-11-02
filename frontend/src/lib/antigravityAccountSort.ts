// Antigravity 账号列表排序的标签页级记忆，与 Codex 账号页（accountListPreferences.ts）同一机制：
// 存 sessionStorage，同一标签页内切换菜单、F5 刷新都会保留，关闭或新开标签页回到默认排序；
// 恢复默认排序时清除存储。单独使用 key，避免和 Codex 页的排序互相覆盖。

export const ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY = "codex2api:antigravity-accounts:sort";

export const ANTIGRAVITY_ACCOUNT_SORT_KEYS = [
  "updated_at",
  "usage",
  "today",
  "requests",
  "created_at",
  "group",
  "id",
] as const;
export type AntigravityAccountSortKey = (typeof ANTIGRAVITY_ACCOUNT_SORT_KEYS)[number];
export type AntigravityAccountSortDir = "asc" | "desc";
export type AntigravityAccountSort = {
  key: AntigravityAccountSortKey;
  dir: AntigravityAccountSortDir;
};

type PreferenceStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

function resolveSessionStorage(storage?: PreferenceStorage): PreferenceStorage | null {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

function isSortKey(value: unknown): value is AntigravityAccountSortKey {
  return (
    typeof value === "string" &&
    (ANTIGRAVITY_ACCOUNT_SORT_KEYS as readonly string[]).includes(value)
  );
}

// 没有记忆或存储内容无效时返回 null，由页面使用自己的默认排序。
export function readAntigravityAccountSort(storage?: PreferenceStorage): AntigravityAccountSort | null {
  try {
    const raw = resolveSessionStorage(storage)?.getItem(ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { key?: unknown; dir?: unknown } | null;
    if (!parsed || !isSortKey(parsed.key)) return null;
    return { key: parsed.key, dir: parsed.dir === "asc" ? "asc" : "desc" };
  } catch {
    return null;
  }
}

// 传 null 表示已恢复默认排序，清除存储。
export function writeAntigravityAccountSort(
  sort: AntigravityAccountSort | null,
  storage?: PreferenceStorage,
): void {
  try {
    const target = resolveSessionStorage(storage);
    if (!target) return;
    if (sort === null) {
      target.removeItem(ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY);
      return;
    }
    target.setItem(
      ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY,
      JSON.stringify({ key: sort.key, dir: sort.dir }),
    );
  } catch {
    // 存储不可用时仍保留内存中的排序。
  }
}
