if (!process.env.JWT_SECRET?.trim()) {
  process.env.JWT_SECRET = 'test-e2e-jwt-secret-do-not-use-in-production';
}
