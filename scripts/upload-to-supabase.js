/**
 * Uploads scraped PCPartPicker data to Supabase.
 *
 * Transforms raw data to match the pcpartpicker_prices table schema
 * and upserts in batches.
 *
 * Required environment variables:
 * - SUPABASE_URL
 * - SUPABASE_SERVICE_KEY
 */

const fs = require('fs')
const path = require('path')

const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Error: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables')
  process.exit(1)
}

// Map scraper category names to our DB category names and file paths
const CATEGORIES = {
  'cpu': { file: 'cpu.json', dbName: 'cpu' },
  'video-card': { file: 'video-card.json', dbName: 'gpu' },
  'memory': { file: 'memory.json', dbName: 'ram' },
  'internal-hard-drive': { file: 'internal-hard-drive.json', dbName: 'storage' },
  'power-supply': { file: 'power-supply.json', dbName: 'psu' }
}

// Transform functions for each category
function transformGPU(item) {
  return {
    category: 'gpu',
    name: item.name,
    price: item.price,
    chipset: item.chipset || null,
    specs: {
      memory_gb: item.memory,
      core_clock_mhz: item.core_clock,
      boost_clock_mhz: item.boost_clock,
      length_mm: item.length
    }
  }
}

function transformCPU(item) {
  // Extract chipset-like identifier from name (e.g., "Ryzen 7 7800X3D" or "Core i7-14700K")
  const chipset = item.name
    .replace(/^AMD\s+/, '')
    .replace(/^Intel\s+/, '')
    .trim()

  return {
    category: 'cpu',
    name: item.name,
    price: item.price,
    chipset: chipset,
    specs: {
      core_count: item.core_count,
      core_clock_ghz: item.core_clock,
      boost_clock_ghz: item.boost_clock,
      microarchitecture: item.microarchitecture,
      tdp_w: item.tdp,
      integrated_graphics: item.graphics,
      smt: item.smt
    }
  }
}

function transformRAM(item) {
  // Create a chipset-like identifier: "DDR5-6000 32GB (2x16GB)"
  const ddrVersion = item.speed?.[0] ? `DDR${item.speed[0]}` : 'DDR'
  const mhz = item.speed?.[1] || 0
  const moduleCount = item.modules?.[0] || 0
  const moduleSize = item.modules?.[1] || 0
  const totalGB = moduleCount * moduleSize
  const chipset = `${ddrVersion}-${mhz} ${totalGB}GB (${moduleCount}x${moduleSize}GB)`

  return {
    category: 'ram',
    name: item.name,
    price: item.price,
    chipset: chipset,
    specs: {
      ddr_version: item.speed?.[0],
      speed_mhz: item.speed?.[1],
      module_count: item.modules?.[0],
      module_size_gb: item.modules?.[1],
      total_gb: totalGB,
      price_per_gb: item.price_per_gb,
      cas_latency: item.cas_latency,
      first_word_latency_ns: item.first_word_latency
    }
  }
}

function transformStorage(item) {
  // Safely get string values (dataset may have non-string values)
  const formFactor = typeof item.form_factor === 'string' ? item.form_factor : ''
  const interfaceType = typeof item.interface === 'string' ? item.interface : ''

  // Determine if SSD or HDD
  const isSSD =
    item.type === 'SSD' ||
    formFactor.includes('M.2') ||
    interfaceType.includes('NVMe')
  const storageType = isSSD ? 'SSD' : 'HDD'

  // Create chipset-like identifier: "1TB NVMe SSD" or "2TB 7200RPM HDD"
  const capacityStr =
    item.capacity >= 1000
      ? `${(item.capacity / 1000).toFixed(0)}TB`
      : `${item.capacity}GB`
  const nvmeStr = interfaceType.includes('NVMe') ? 'NVMe ' : ''
  const chipset = `${capacityStr} ${nvmeStr}${storageType}`

  return {
    category: 'storage',
    name: item.name,
    price: item.price,
    chipset: chipset,
    specs: {
      capacity_gb: item.capacity,
      type: storageType,
      form_factor: item.form_factor,
      interface: item.interface,
      cache_mb: item.cache,
      price_per_gb: item.price_per_gb
    }
  }
}

function transformPSU(item) {
  // Create chipset-like identifier: "850W 80+ Gold"
  const efficiency = item.efficiency || ''
  const chipset = `${item.wattage}W ${efficiency}`.trim()

  return {
    category: 'psu',
    name: item.name,
    price: item.price,
    chipset: chipset,
    specs: {
      wattage: item.wattage,
      efficiency: item.efficiency,
      modular: item.modular,
      type: item.type
    }
  }
}

// Deduplicate items by name, keeping the one with the lowest price
function deduplicateByName(items) {
  const seen = new Map()

  for (const item of items) {
    const existing = seen.get(item.name)
    if (!existing) {
      seen.set(item.name, item)
    } else {
      // Keep the one with the lower price (prefer non-null prices)
      if (item.price !== null && (existing.price === null || item.price < existing.price)) {
        seen.set(item.name, item)
      }
    }
  }

  return Array.from(seen.values())
}

// Upsert batch to Supabase via REST API
async function upsertBatch(items) {
  const url = `${SUPABASE_URL}/rest/v1/pcpartpicker_prices?on_conflict=category,name`

  const body = items.map(item => ({
    category: item.category,
    name: item.name,
    price: item.price,
    chipset: item.chipset,
    specs: item.specs,
    updated_at: new Date().toISOString()
  }))

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal,resolution=merge-duplicates'
    },
    body: JSON.stringify(body)
  })

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`Supabase upsert failed: ${response.status} - ${errorText}`)
  }
}

async function uploadCategory(scraperCategory, config) {
  const filePath = path.join(__dirname, '../data/json', config.file)

  if (!fs.existsSync(filePath)) {
    console.error(`  ❌ File not found: ${config.file}`)
    return 0
  }

  const rawData = JSON.parse(fs.readFileSync(filePath, 'utf8'))

  // Transform based on category
  let transformed
  switch (config.dbName) {
    case 'gpu':
      transformed = rawData.map(transformGPU)
      break
    case 'cpu':
      transformed = rawData.map(transformCPU)
      break
    case 'ram':
      transformed = rawData.map(transformRAM)
      break
    case 'storage':
      transformed = rawData.map(transformStorage)
      break
    case 'psu':
      transformed = rawData.map(transformPSU)
      break
    default:
      console.error(`  ❌ Unknown category: ${config.dbName}`)
      return 0
  }

  // Deduplicate
  const originalCount = transformed.length
  transformed = deduplicateByName(transformed)
  const dedupedCount = transformed.length

  console.log(`  📦 ${config.dbName.toUpperCase()}: ${originalCount} items (${originalCount - dedupedCount} duplicates removed)`)

  // Batch upsert in chunks of 500
  const BATCH_SIZE = 500
  for (let i = 0; i < transformed.length; i += BATCH_SIZE) {
    const batch = transformed.slice(i, i + BATCH_SIZE)
    await upsertBatch(batch)
    process.stdout.write(`  ⬆️  Uploaded batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(transformed.length / BATCH_SIZE)}\r`)
  }
  console.log('')

  return dedupedCount
}

async function main() {
  console.log('PCPartPicker Data Upload to Supabase')
  console.log('====================================\n')

  let totalUploaded = 0

  for (const [scraperCategory, config] of Object.entries(CATEGORIES)) {
    const count = await uploadCategory(scraperCategory, config)
    totalUploaded += count
  }

  console.log('\n====================================')
  console.log(`✅ Upload complete! ${totalUploaded} items upserted to Supabase.`)
}

main().catch(error => {
  console.error('❌ Upload failed:', error.message)
  process.exit(1)
})
