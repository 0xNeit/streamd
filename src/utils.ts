let nextId = 0;

export function generateId(): string {
  return `streamd_${++nextId}_${Date.now()}`;
}
