/**
 * Validates scraped PCPartPicker data before uploading to Supabase.
 *
 * Checks:
 * - All required category files exist
 * - Each category has minimum number of items
 * - Prices are within reasonable bounds
 */

const fs = require('fs')
const path = require('path')

// Map scraper category names to our DB category names
const CATEGORIES = {
  'cpu': { file: 'cpu.json', dbName: 'cpu', minItems: 100 },
  'video-card': { file: 'video-card.json', dbName: 'gpu', minItems: 100 },
  'memory': { file: 'memory.json', dbName: 'ram', minItems: 200 },
  'internal-hard-drive': { file: 'internal-hard-drive.json', dbName: 'storage', minItems: 200 },
  'power-supply': { file: 'power-supply.json', dbName: 'psu', minItems: 100 }
}

const MIN_PRICE = 5      // No item should be under $5
const MAX_PRICE = 10000  // No item should be over $10,000

let hasErrors = false
let totalItems = 0

console.log('PCPartPicker Data Validation')
console.log('============================\n')

for (const [category, config] of Object.entries(CATEGORIES)) {
  const filePath = path.join(__dirname, '../data/json', config.file)

  // Check file exists
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ${category}: Missing file ${config.file}`)
    hasErrors = true
    continue
  }

  // Parse JSON
  let data
  try {
    data = JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch (error) {
    console.error(`❌ ${category}: Failed to parse ${config.file} - ${error.message}`)
    hasErrors = true
    continue
  }

  // Check minimum items
  if (data.length < config.minItems) {
    console.error(`❌ ${category}: Only ${data.length} items (expected ${config.minItems}+)`)
    hasErrors = true
  }

  // Check for invalid prices
  const itemsWithPrice = data.filter(item => item.price !== null && item.price !== undefined)
  const invalidPrices = itemsWithPrice.filter(item =>
    item.price < MIN_PRICE || item.price > MAX_PRICE
  )

  if (invalidPrices.length > 10) {
    // Allow some outliers but flag if too many
    console.warn(`⚠️  ${category}: ${invalidPrices.length} items with suspicious prices (outside $${MIN_PRICE}-$${MAX_PRICE})`)
  }

  // Check for items without prices
  const itemsWithoutPrice = data.length - itemsWithPrice.length
  const priceRatio = (itemsWithPrice.length / data.length * 100).toFixed(1)

  totalItems += data.length
  console.log(`✅ ${category}: ${data.length} items (${itemsWithPrice.length} with prices, ${priceRatio}%)`)
}

console.log('\n============================')
console.log(`Total items: ${totalItems}`)

if (hasErrors) {
  console.error('\n❌ Validation FAILED! Not uploading to Supabase.')
  process.exit(1)
}

console.log('\n✅ All data validated successfully!')
process.exit(0)
