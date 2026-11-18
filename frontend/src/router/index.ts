import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import SourceConfigWorkbench from '@/views/SourceConfigWorkbench.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 页面建模工作台额外提供配置版本快照与批次绑定核对视图
    { path: '/page_model/configs', name: 'page-model-configs', component: SourceConfigWorkbench },
    { path: '/page_model', name: 'page-model', component: WorkbenchView, props: { moduleKey: 'page_model' } },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
