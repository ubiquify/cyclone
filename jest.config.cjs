module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  testMatch: ["**/dist/__tests__/**/*.test.(ts|js)"],
  globals: {
    "ts-jest": {
      tsconfig: "tsconfig.json",
    },
  },
};
